import asyncio
import logging
import multiprocessing
from typing import Dict, List
import time

class Controller:
    UPDATE_TIME = 0.1
    def __init__(self) -> None:
        self.apps : Dict[str, App] = {}
        self.responses : Dict[int, str] = {}
        self.event_id = 0
        self.logger = logging.getLogger('Process controller')
    
    def register_app(self, parent, name):
        """ регистрирует новый процесс с именем. В дальнейшем по этому имени происходит взаимодействие"""
        app = App(parent, name)
        self.apps[name] = app
        return app
    
    def loop(self):
        """ цикл в котором проверяются все соединения с процессами на наличие данных"""
        while True:
            for app in self.apps.values():
                if app.parent_pipe.poll():
                    self._event_handler(app.parent_pipe.recv())
            time.sleep(self.UPDATE_TIME)
    
    def _event_handler(self, event):
        """ обработчик всех приходящих в главный процесс событий"""
        print(event)
        if event['type'] == 'queue':
            self._add_in_queue(event)
        elif event['type'] == 'event':
            self._event_with_response(event)
        elif event['type'] == 'response':
            self._answer_response(event)
        elif event['type'] == 'exists':
            self._check_app_exist(event)
        else:
            self.logger.error(f'unknown event type from {event["from"]}: {event["type"]}')
    
    def _add_in_queue(self, event):
        """ добавляет данные пришедшие от одного процесса в очередь другого"""
        self.apps[event['to']].queue.put_nowait(event['data'])
    
    def _event_with_response(self, event):
        """ создает события для которого нужно вернуть результат отправителю """
        event['id'] = self.event_id
        self.apps[event['from']].parent_pipe.send(self.event_id)
        self.event_id += 1
        self.apps[event['to']].parent_pipe.send(event)
        self.responses[event['id']] = event['from']
    
    def _answer_response(self, event):
        """ возвращает результат отправителю """
        print('response', event)
        to = self.responses.pop(event['id'])
        self.apps[to].parent_pipe.send(event)
    
    def _check_app_exist(self, event) -> bool:
        exists = event['name'] in self.apps
        self.apps[event['from']].parent_pipe.send(exists)


class App:
    """ класс представляющий процесс в котроллере """
    UPDATE_TIME = 0.1
    def __init__(self, parent, name) -> None:
        self.name = name
        self.parent_pipe, self.pipe = multiprocessing.Pipe()
        self.queue = multiprocessing.Queue()
        self.event_handlers : Dict[str, Handler] = {}
        self.parent = parent
        self.queue_handler = None
        self.is_locked = False
    
    async def update_loop(self):
        while True:
            if not self.is_locked:
                if self.pipe.poll():
                    print(self.name, 'poll')
                    await self._event_handler(self.pipe.recv())
                if not self.queue.empty():
                    if self.queue_handler is not None:
                        while not self.queue.empty():
                            self.queue_handler(self.queue.get())
            await asyncio.sleep(self.UPDATE_TIME)
    
    def wait_for_init(self, name):
        event = {'type': 'exists', 'name': name, 'from': self.name}
        while True:
            self.pipe.send(event)
            if self.pipe.recv():
                print('wait for', name)
                break
            time.sleep(self.UPDATE_TIME)

    
    async def send_event(self, app_name:str, event_name, **params):
        """ отправляет запрос для получения результата из другого процесса """
        event = self.event_template('event', app_name, event=event_name, **params)
        self.is_locked = True
        self.pipe.send(event)
        # в цикле ждем пока не вернется id зарегестрированной задачи
        print(self.name, 'wait for id')
        while True:
            data = self.pipe.recv()
            if isinstance(data, int):
                id = data
                break
            else:
                await self._event_handler(data)
        print(self.name, 'wait for data')
        # ожидаем пока не придут данные по запросу
        while True:
            data = self.pipe.recv()
            if isinstance(data, dict):
                if 'data' in data:
                    break
                else:
                    await self._event_handler(data)
        self.is_locked = False
        return data
    
    def add_in_queue(self, app_name, data):
        """ отправляет данные в очередь другого процесса """
        event = self.event_template('queue', app_name, data=data)
        self.pipe.send(event)
    
    def event_template(self, type, to_app, **kwargs):
        """ генерирует шаблон для события """
        event = {'type': type, 'from': self.name, 'to': to_app}
        event.update(kwargs)
        return event

    def register_handler(self, event:str, callback, is_async=False):
        """ регистрирует обработчик для пришедших событий """
        self.event_handlers[event] = Handler(callback, is_async=is_async)

    async def _event_handler(self, event:dict):
        """ обработчик пришедших событий """
        event['type'] = 'response'
        handler = self.event_handlers[event['event']]
        if handler.is_async:
            result:dict = await handler.func()
        else:
            result:dict = handler.func()
        event['data'] = result
        self.pipe.send(event)

class Handler:
    def __init__(self, func, is_async=False) -> None:
        self.func = func
        self.is_async = is_async