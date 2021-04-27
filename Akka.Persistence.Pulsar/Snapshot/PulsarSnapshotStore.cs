using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using System;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Akka.Configuration;
using IdentityModel;
using SharpPulsar.Schemas;
using SharpPulsar.Configuration;
using SharpPulsar;
using SharpPulsar.User;
using SharpPulsar.Sql;
using SharpPulsar.Messages;
using SharpPulsar.Sql.Client;
using SharpPulsar.Sql.Message;

namespace Akka.Persistence.Pulsar.Snapshot
{
    /// <summary>
    ///     Pulsar-backed snapshot store for Akka.Persistence.
    /// </summary>
    /// 
    
    public class PulsarSnapshotStore : SnapshotStore
    {
        private readonly CancellationTokenSource _pendingRequestsCancellation;
        private readonly PulsarSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();
        private readonly PulsarSystem _pulsarSystem;
        private readonly PulsarClient _client;
        private readonly Sql<SqlData> _sql;
        private readonly ClientOptions _sqlClientOptions;
        public static readonly ConcurrentDictionary<string, Producer<SnapshotEntry>> _producers = new ConcurrentDictionary<string, Producer<SnapshotEntry>>();
        private static readonly Type SnapshotType = typeof(Serialization.Snapshot);
        private readonly Serializer _serializer;
        private readonly AvroSchema<SnapshotEntry> _snapshotEntrySchema;


        //public Akka.Serialization.Serialization Serialization => _serialization ??= Context.System.Serialization;

        public PulsarSnapshotStore(Config config) : this(new PulsarSettings(config))
        {

        }

        public PulsarSnapshotStore(PulsarSettings settings)
        {
            _pendingRequestsCancellation = new CancellationTokenSource();
            _snapshotEntrySchema = AvroSchema<SnapshotEntry>.Of(typeof(SnapshotEntry));
            _serializer = Context.System.Serialization.FindSerializerForType(SnapshotType);
            _settings = settings;
            _sqlClientOptions = new ClientOptions 
            { 
                Server = settings.PrestoServer,
                Catalog = "pulsar",
                Schema = $"{settings.Tenant}/{settings.Namespace}"
            };
            _pulsarSystem = settings.CreateSystem(Context.System);
            _client = _pulsarSystem.NewClient();
            _sql = PulsarSystem.NewSql();
        }
        
        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            //use admin api to implement - maybe
            await Task.CompletedTask;
        }

        //use admin api to implement - maybe
        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            await Task.CompletedTask;
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            SelectedSnapshot shot = null;
            _sqlClientOptions.Execute = $"select Id, PersistenceId, SequenceNr, Timestamp, Snapshot from snapshot WHERE PersistenceId = {persistenceId} AND SequenceNr <= {criteria.MaxSequenceNr} AND Timestamp <= {criteria.MaxTimeStamp.ToEpochTime()} ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT 1";
            _sql.SendQuery(new SqlQuery(_sqlClientOptions, e => { _log.Error(e.ToString()); }, l => { _log.Info(l); }));
            var response = await _sql.ReadQueryResultAsync(TimeSpan.FromSeconds(5));
            var data = response.Response;
            switch (data)
            {
                case DataResponse dr:
                    return ToSelectedSnapshot(JsonSerializer.Deserialize<SnapshotEntry>(JsonSerializer.Serialize(dr.Data)));
                case StatsResponse sr:
                    _log.Info(JsonSerializer.Serialize(sr, new JsonSerializerOptions { WriteIndented = true }));
                    return shot;
                case ErrorResponse er:
                    _log.Error(er.Error.FailureInfo.Message);
                    return shot;
                default:
                    return shot;
            }
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var producer = await GetProducer(metadata.PersistenceId);
            var snapshotEntry = ToSnapshotEntry(metadata, snapshot);
            await producer.SendAsync(snapshotEntry);
        }

        private async ValueTask<Producer<SnapshotEntry>> CreateSnapshotProducer(string topic, string persistenceid)
        {
            var producerConfig = new ProducerConfigBuilder<SnapshotEntry>()
                   .ProducerName($"snapshot-{persistenceid}")
                   .Topic(topic)
                   .Schema(_snapshotEntrySchema)
                   .SendTimeout(10000);
            var producer = await _client.NewProducerAsync(_snapshotEntrySchema, producerConfig);
            _producers[persistenceid] = producer;
            return producer;
        }
        private async ValueTask<Producer<SnapshotEntry>> GetProducer(string persistenceid)
        {
            var topic = $"persistent://{_settings.Tenant}/{_settings.Namespace}/snapshot".ToLower();
            if (_producers.TryGetValue(persistenceid, out var producer))
            {
                return producer;
            }
            else
            {
                return await CreateSnapshotProducer(topic, persistenceid);
            }
        }
        protected override void PostStop()
        {
            base.PostStop();

            // stop all operations executed in the background
            _pendingRequestsCancellation.Cancel();
            _client.Shutdown();
        }
        
        private object Deserialize(byte[] bytes)
        {
            return ((Serialization.Snapshot)_serializer.FromBinary(bytes, SnapshotType)).Data;
        }

        private byte[] Serialize(object snapshotData)
        {
            return _serializer.ToBinary(new Serialization.Snapshot(snapshotData));
        }
        private SnapshotEntry ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var binary = Serialize(snapshot);

            return new SnapshotEntry
            {
                Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                PersistenceId = metadata.PersistenceId,
                SequenceNr = metadata.SequenceNr,
                Snapshot = Convert.ToBase64String(binary),
                Timestamp = metadata.Timestamp.ToEpochTime()
            };
        }

        private SelectedSnapshot ToSelectedSnapshot(SnapshotEntry entry)
        {
            var snapshot = Deserialize(Convert.FromBase64String(entry.Snapshot));
            return new SelectedSnapshot(new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr), snapshot);

        }
    }
}
