using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar;
using SharpPulsar.Configuration;
using SharpPulsar.Messages;
using SharpPulsar.Schemas;
using SharpPulsar.Sql;
using SharpPulsar.Sql.Client;
using SharpPulsar.Sql.Message;
using SharpPulsar.User;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournalExecutor
    {
        private readonly PulsarSystem _pulsarSystem;
        private readonly PulsarClient _client;
        private readonly Sql<SqlData> _sql;
        private readonly ClientOptions _sqlClientOptions;
        private readonly ILoggingAdapter _log ;
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);
        public static readonly ConcurrentDictionary<string, IActorRef> Producers = new ConcurrentDictionary<string, IActorRef>();

        private readonly AvroSchema<JournalEntry> _journalEntrySchema;
        private readonly List<string> _activeReplayTopics;

        public PulsarJournalExecutor(ActorSystem actorSystem, PulsarSettings settings, ILoggingAdapter log, Serializer serializer, CancellationTokenSource cancellation)
        {
            _activeReplayTopics = new List<string>();
            Settings = settings;
            _log = log;
            _serializer = serializer; 
            _journalEntrySchema = AvroSchema<JournalEntry>.Of(typeof(JournalEntry), new Dictionary<string, string>());
            _sqlClientOptions = new ClientOptions
            {
                Server = settings.ServiceUrl,
                Catalog = "pulsar",
                Schema = $"{settings.Tenant}/{settings.Namespace}"
            };
            _pulsarSystem = settings.CreateSystem(actorSystem);
            _client = _pulsarSystem.NewClient();
            _sql = PulsarSystem.NewSql();
        }
        public async ValueTask ReplayMessages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            var topic = $"journal-{persistenceId}".ToLower();
            _sqlClientOptions.Execute = $"select Id, PersistenceId, __sequence_id__ as SequenceNr, IsDeleted, Payload, Ordering, Tags from {topic} WHERE SequenceNr BETWEEN {fromSequenceNr} AND {toSequenceNr} ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT {max}";
            _sql.SendQuery(new SqlQuery(_sqlClientOptions, e => { _log.Error(e.ToString()); }, l => { _log.Info(l); }));

            _sqlClientOptions.Execute = string.Empty;
            await foreach (var data in _sql.ReadResults(TimeSpan.FromSeconds(30)))
            {
                switch (data.Response)
                {
                    case DataResponse dr:
                        var journal = JsonSerializer.Deserialize<JournalEntry>(JsonSerializer.Serialize(dr.Data));                       
                        var payload = journal.Payload;
                        var der = Deserialize(payload);
                        recoveryCallback(der);
                        break;
                    case StatsResponse sr:
                        if(_log.IsDebugEnabled)
                            _log.Info(JsonSerializer.Serialize(sr, new JsonSerializerOptions { WriteIndented = true }));
                        break;
                    case ErrorResponse er:
                        _log.Error(er.Error.FailureInfo.Message);
                        break;
                    default:
                        break; 
                }
            }
        }
        public async ValueTask<long> ReadHighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            try
            {
                var topic = $"journal-{persistenceId}";
                _sqlClientOptions.Execute = $"select __sequence_id__ as Id from {topic}  ORDER BY __sequence_id__ as DESC LIMIT 1";
                _sql.SendQuery(new SqlQuery(_sqlClientOptions, e => { _log.Error(e.ToString()); }, l => { _log.Info(l); }));
                var response = await _sql.ReadQueryResultAsync(TimeSpan.FromSeconds(30));
                _sqlClientOptions.Execute = string.Empty;
                var data = response.Response;
                switch (data)
                {
                    case DataResponse dr:
                        return (long)dr.Data["Id"];
                    case StatsResponse sr:
                        if(_log.IsDebugEnabled)
                            _log.Info(JsonSerializer.Serialize(sr, new JsonSerializerOptions { WriteIndented = true }));
                        return 0;
                    case ErrorResponse er:
                        _log.Error(er.Error.FailureInfo.Message);
                        return -1;
                    default:
                        return 0;
                }
            }
            catch (Exception e)
            {
                return 0;
            }
        }
        private void CreateJournalProducer(string topic, string persistenceid)
        {
            var p = Producers.FirstOrDefault(x => x.Key == topic).Value;
            if (p == null)
            {
                var producerConfig = new ProducerConfigBuilder<JournalEntry>()
                    .ProducerName($"journal-{persistenceid}")
                    .Topic(topic)
                    .Schema(_journalEntrySchema)
                    .SendTimeout(10000);
                var producer = Client.PulsarProducer(new CreateProducer(_journalEntrySchema, producerConfig));
                if (Producers.ContainsKey(producer.Topic))
                    Producers[producer.Topic] = producer.Producer;
                else
                {
                    Producers[producer.Topic] = producer.Producer;
                }
            }
        }
        internal (string topic, IActorRef producer) GetProducer(string persistenceId, string type)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/{type}-{persistenceId}".ToLower();
            var p = Producers.FirstOrDefault(x => x.Key == topic).Value;
            if (p == null)
            {
                CreateJournalProducer(topic, persistenceId);
                return GetProducer(persistenceId, type);
            }
            return (topic, p);
        }
        internal IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation)_serializer.FromBinary(bytes, PersistentRepresentationType);
        }

        public PulsarSystem Client { get; }
        internal PulsarSettings Settings { get; }

        internal long GetMaxOrderingId(ReplayTaggedMessages replay)
        {
            var topic = $"{Settings.TopicPrefix.TrimEnd('/')}/journal-*";
            var numb = Client.EventSource(new GetNumberOfEntries(topic, Settings.AdminUrl));
            return numb.Max.Value;
        }
        private string SubQuery(string persistenceId)
        {

        }
    }
}
