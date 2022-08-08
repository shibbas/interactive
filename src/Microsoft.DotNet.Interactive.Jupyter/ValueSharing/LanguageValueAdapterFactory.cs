﻿using Microsoft.DotNet.Interactive.Jupyter.Messaging;
using Microsoft.DotNet.Interactive.Jupyter.Messaging.Comms;
using Microsoft.DotNet.Interactive.Jupyter.Protocol;
using System;
using System.Reactive.Threading.Tasks;
using System.Threading.Tasks;
using System.Reactive.Linq;

namespace Microsoft.DotNet.Interactive.Jupyter.ValueSharing
{
    internal class LanguageValueAdapterFactory : IGetValueAdapter
    {
        private static string TargetName = "value_adapter_comm";

        private readonly CommsManager _commsManager;
        private readonly IMessageSender _sender;
        private readonly IMessageReceiver _receiver;

        public LanguageValueAdapterFactory(IMessageSender sender, IMessageReceiver receiver, CommsManager commsManager)
        {
            _sender = sender ?? throw new ArgumentNullException(nameof(sender));
            _receiver = receiver ?? throw new ArgumentNullException(nameof(receiver));
            _commsManager = commsManager ?? throw new ArgumentNullException(nameof(commsManager));
        }

        public async Task<IValueAdapter> GetValueAdapter(KernelInfo kernelInfo)
        {
            var language = kernelInfo.LanguageName;
            switch(language?.ToLowerInvariant())
            {
                case LanguageNameValues.Python:
                    return await CreateValueAdapterAsync(new PythonValueAdapterCommTarget());
                default:
                    return null;
            }
        }

        private async Task<IValueAdapter> CreateValueAdapterAsync(IValueAdapterCommDefinition definition)
        {
            var commTargetInitialized = await RunOnKernelAsync(definition.GetTargetDefinition(TargetName));
            if (commTargetInitialized)
            {
                var agent = await _commsManager.OpenCommAsync(TargetName);

                if (agent is not null) 
                {
                    var response = await agent.Messages
                        .TakeUntilMessageType(JupyterMessageContentTypes.CommMsg, JupyterMessageContentTypes.CommClose)
                        .ToTask();

                    if (response is CommMsg messageReceived)
                    {
                        var adapterMessage = ValueAdapterMessageExtensions.FromDataDictionary(messageReceived.Data);
                        if (adapterMessage is InitializedEvent)
                        {
                            return new CommValueAdapter(agent);
                        }
                    }
                }
            }

            return null;
        }

        private async Task<bool> RunOnKernelAsync(string code)
        {
            var executeRequest = Messaging.Message.Create(new ExecuteRequest(code,
                                                                             silent: true,
                                                                             storeHistory: false));

            var executeReply = _receiver.Messages.ChildOf(executeRequest)
                                    .SelectContent()
                                    .TakeUntilMessageType(JupyterMessageContentTypes.ExecuteReply, JupyterMessageContentTypes.Error);
            // run until we get a definitive pass or fail

            await _sender.SendAsync(executeRequest);
            var reply = await executeReply.ToTask();

            return reply is ExecuteReplyOk;
        }
    }
}
