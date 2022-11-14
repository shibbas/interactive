﻿import json
def __get_dotnet_coe_comm_handler(): 
    
    class CommandEventCommTarget:
        __control_comm = None
        __coe_handler = None
        _is_debug = False

        def handle_control_comm_opened(self, comm, msg):
            self.__control_comm = comm
            if (comm is not None):
                self.__control_comm.on_msg(self.handle_control_comm_msg)

            self.__coe_handler = CommandEventHandler()
            self.__send_control_comm_msg(self.__coe_handler.is_ready())

        def handle_control_comm_msg(self, msg):
            # This shouldn't happen unless someone calls this method manually
            if self.__control_comm is None and not self._is_debug:
                raise RuntimeError('Control comm has not been properly opened')

            data = msg['content']['data']
            response = self.__coe_handler.handle_command(data)
            self.__send_control_comm_msg(response)

        def __send_control_comm_msg(self, payload):
            if self._is_debug:
                print (payload)
            else:
                self.__control_comm.send(payload)
    
    
    class CommandEventHandler:          
        __exclude_types = ["<class 'module'>"]        
        
        def handle_command(self, data):
            try:
                commandOrEvent = json.loads(data['commandOrEvent'])
                self.__debugLog('handle_command.last_data_recv', commandOrEvent)
                
                commandType = commandOrEvent['commandType']
                
                envelop = None
                if (commandType == SendValue.__name__):
                    envelop = self.__handle_send_value(commandOrEvent)
                elif (commandType == RequestValue.__name__):
                    envelop = self.__handle_request_value(commandOrEvent)
                elif (commandType == RequestValueInfos.__name__):
                    envelop = self.__handle_request_value_infos(commandOrEvent)
                else: 
                    envelop = EventEnvelope(CommandFailed(f'command "{commandType}" not supported'))
                
                return envelop.payload()
                
            except Exception as e: 
                self. __debugLog('handle_command.commandFailed', e)
                return EventEnvelope(CommandFailed(f'failed to process comm data. {str(e)}')).payload()

        def __handle_request_value_infos(self, command):
            results_who_ls = %who_ls
            variables = globals()
            results = [KernelValueInfo(x, str(type(variables[x]))) for x in results_who_ls ]
            results = list(filter(lambda v: v.nativeType not in self.__exclude_types, results))
            
            if (results is not None):
                return EventEnvelope(ValueInfosProduced(results), command)
            
            return EventEnvelope(CommandFailed(f'Failed to get variables.'))
            
        def __handle_request_value(self, command):
            requestValue = RequestValue(command['command'])
            name = requestValue.name
            mimeType = requestValue.mimeType
            
            if (name not in globals()):
                return EventEnvelope(CommandFailed(f'Variable "{name}" not found.'))
            
            rawValue = globals()[name]
            
            try: 
                import pandas as pd; 
                if (isinstance(rawValue, pd.DataFrame)):
                    mimeType = 'application/table-schema+json'
                    rawValue = rawValue.to_dict('records')
            except Exception as e: 
                self. __debugLog('__handle_request_value.dataframe.error', e)
                pass

            formattedValue = FormattedValue(mimeType) # This will be formatted in the .NET kernel
            
            if (rawValue is not None): 
                return EventEnvelope(ValueProduced(name, rawValue, formattedValue), command)
            
            return EventEnvelope(CommandFailed(f'Failed to get value for "{name}"'))
        
        def __handle_send_value(self, command):
            sendValue = SendValue(command['command'])
            mimeType = sendValue.formattedValue['mimeType']
            name = sendValue.name
            rawValue = sendValue.formattedValue['value']
            resultValue = None
            
            if (not str.isidentifier(name)):
                return EventEnvelope(CommandFailed(f'Invalid Identifier: "{name}"'))
        
            if (mimeType == 'application/json'):
                import json; resultValue = json.loads(rawValue)
            elif (mimeType == 'application/table-schema+json'):
                import json; resultValue = json.loads(rawValue)
                try:
                    import pandas as pd; resultValue = pd.DataFrame(data=resultValue['data'])
                except Exception as e:
                    self.__debugLog('__handle_send_value.dataframe.error', e)
                    return EventEnvelope(CommandFailed(f'Cannot create pandas dataframe for: "{name}". {str(e)}'))
                
            if (resultValue is not None): 
                self.__setVariable(name, resultValue) 
                return EventEnvelope(CommandSucceeded())
            
            return EventEnvelope(CommandFailed(f'Failed to set value for "{name}". "{mimeType}" mimetype not supported.'))
        
        def is_ready(self):
            return EventEnvelope(KernelReady()).payload()
        
        @staticmethod
        def __setVariable(name, value):
            globals()[name] = value
        
        @staticmethod
        def __debugLog(event, message):
            globals()[f'__log__coe_handler.{str(event)}'] = message
    
    
    class KernelCommand: 
        pass

    class SendValue(KernelCommand): 
        def __init__(self, entries):
            self.__dict__.update(**entries)

    class RequestValue(KernelCommand):
        def __init__(self, entries):
            self.__dict__.update(**entries)

    class RequestValueInfos(KernelCommand):
        def __init__(self, entries):
            self.__dict__.update(**entries)
            
    class FormattedValue:
        def __init__(self, mimeType = 'application/json', value = None):
            self.mimeType = mimeType
            self.value = value
    
    class KernelValueInfo:
        def __init__(self, name, nativeType = None):
            self.name = name
            self.nativeType = nativeType
            
    class KernelEvent:
        pass

    class KernelReady(KernelEvent):
        pass

    class CommandSucceeded(KernelEvent):
        pass

    class CommandFailed(KernelEvent):
        def __init__(self, message = None):
            self.message = message

    class ValueProduced(KernelEvent):
        def __init__(self, name, value, formattedValue: FormattedValue):
            self.name = name
            self.value = value 
            self.formattedValue = formattedValue
    
    class ValueInfosProduced(KernelEvent):
        def __init__(self, valueInfos: [KernelValueInfo]):
            self.valueInfos = valueInfos
            
    class Envelope:
        def payload(self):
            return { 'commandOrEvent': self.__to_json_string(self) }

        @staticmethod
        def __to_json_string(obj):
            return json.dumps(obj, default=lambda o: o.__dict__)

    class EventEnvelope(Envelope):
        def __init__(self, event: KernelEvent = None, command = None):
            self.event = event
            self.eventType = type(event).__name__
            self.command = command

        def payload(self):
            ret = super().payload()
            ret['type'] = 'event'
            return ret
    
    return CommandEventCommTarget()

get_ipython().kernel.comm_manager.register_target('dotnet_coe_handler_comm', __get_dotnet_coe_comm_handler().handle_control_comm_opened)