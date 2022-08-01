﻿using Microsoft.DotNet.Interactive.Commands;
using Microsoft.DotNet.Interactive.Connection;
using Microsoft.DotNet.Interactive.Events;
using Microsoft.DotNet.Interactive.Jupyter.Messaging;
using Microsoft.DotNet.Interactive.Jupyter.ValueSharing;
using Microsoft.DotNet.Interactive.ValueSharing;
using System;
using System.Collections.Concurrent;
using System.Drawing.Text;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.DotNet.Interactive.Jupyter.Connection
{
    internal class MessageToCommandAndEventConnector : IKernelCommandAndEventSender, IKernelCommandAndEventReceiver, ICommandExecutionContext, IDisposable
    {
        private readonly Subject<CommandOrEvent> _commandOrEventsSubject;
        private readonly Uri _targetUri;
        private readonly CompositeDisposable _disposables;
        private readonly IMessageSender _sender;
        private readonly IMessageReceiver _receiver;

        private readonly ConcurrentDictionary<Type, Func<KernelCommand, ICommandExecutionContext, CancellationToken, Task>> _dynamicHandlers = new();
        private readonly KernelValueHandler _kernelValueHandler = new();
        

        public MessageToCommandAndEventConnector(IMessageSender messageSender, IMessageReceiver messageReceiver, Uri targetUri)
        {
            _commandOrEventsSubject = new Subject<CommandOrEvent>();
            _targetUri = targetUri;
            _receiver = messageReceiver;
            _sender = messageSender;

            var submitCodeHandler = new SubmitCodeHandler(messageSender, messageReceiver);
            var requestKernelInfoHandler = new RequestKernelInfoHandler(messageSender, messageReceiver);
            var completionsHandler = new RequestCompletionsHandler(messageSender, messageReceiver);
            var hoverTipHandler = new RequestHoverTextHandler(messageSender, messageReceiver);

            RegisterCommandHandler<SubmitCode>(submitCodeHandler.HandleCommandAsync);
            RegisterCommandHandler<RequestKernelInfo>(requestKernelInfoHandler.HandleCommandAsync);
            RegisterCommandHandler<RequestCompletions>(completionsHandler.HandleCommandAsync);
            RegisterCommandHandler<RequestHoverText>(hoverTipHandler.HandleCommandAsync);

            _disposables = new CompositeDisposable
            {
                _commandOrEventsSubject
            };
        }

        public Uri RemoteHostUri => _targetUri;

        public IValueSupport ValueHandler { get; private set; }

        public void Dispose()
        {
            _disposables.Dispose();
        }

        public void RegisterCommandHandler<TCommand>(Func<TCommand, ICommandExecutionContext, CancellationToken, Task> handler)
            where TCommand : KernelCommand
        {
            _dynamicHandlers[typeof(TCommand)] = (command, context, token) => handler((TCommand)command, context, token);
        }


        private Func<KernelCommand, ICommandExecutionContext, CancellationToken, Task> TryGetDynamicHandler(KernelCommand command)
        {
            if (_dynamicHandlers.TryGetValue(command.GetType(), out var handler))
            {
                return handler;
            }
            return null;
        }

        public void Publish(KernelEvent kernelEvent)
        {
            var commandOrEvent = new CommandOrEvent(kernelEvent);
            _commandOrEventsSubject.OnNext(commandOrEvent);
        }

        public async Task SendAsync(KernelCommand kernelCommand, CancellationToken cancellationToken)
        {
            var handler = TryGetDynamicHandler(kernelCommand);
            if (handler != null)
            {
                await handler(kernelCommand, this, cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                //TODO: trigger an explicit kernel interrupt as well to make sure the out-of-proc kernel 
                // stops any running executions.
            }
        }

        public Task SendAsync(KernelEvent kernelEvent, CancellationToken cancellationToken)
        {
            // TODO: could be used to translate events to jupyter message replies to the 
            // jupyter front end. 
            throw new NotImplementedException();
        }

        public IDisposable Subscribe(IObserver<CommandOrEvent> observer)
        {
            return _commandOrEventsSubject.Select(coe => UpdateCommandOrEvent(coe)).Subscribe(observer);
        }

        private CommandOrEvent UpdateCommandOrEvent(CommandOrEvent coe)
        {
            CommandOrEvent updated = coe;
            if (coe.Event is KernelInfoProduced e)
            {
                ValueHandler = _kernelValueHandler.GetValueSupport(e.KernelInfo.LanguageName, _sender, _receiver);

                if (ValueHandler is ISupportGetValue getValueHandler)
                {
                    SupportGetValue(getValueHandler);
                    e.KernelInfo.SupportedKernelCommands.Add(new(nameof(RequestValue)));
                    e.KernelInfo.SupportedKernelCommands.Add(new(nameof(RequestValueInfos)));
                    updated = new CommandOrEvent(new KernelInfoProduced(e.KernelInfo, e.Command));
                }
            }

            return updated;
        }

        private void SupportGetValue(ISupportGetValue languageValueHandler)
        {
            var valueHandler = new RequestValueHandler(languageValueHandler);
            RegisterCommandHandler<RequestValue>(valueHandler.HandleRequestValueAsync);
            RegisterCommandHandler<RequestValueInfos>(valueHandler.HandleRequestValueInfosAsync);
        }
    }
}
