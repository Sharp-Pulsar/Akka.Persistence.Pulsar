﻿using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence.Pulsar.Query;
using Akka.Serialization;
using SharpPulsar;
using SharpPulsar.Messages;
using SharpPulsar.Sql;
using SharpPulsar.Sql.Client;
using SharpPulsar.Sql.Message;
using SharpPulsar.User;

namespace Akka.Persistence.Pulsar.Journal
{
    public sealed class PulsarJournalExecutor
    {
        private readonly Sql<SqlData> _sql;
        private readonly ClientOptions _sqlClientOptions;
        private readonly ILoggingAdapter _log ;
        private readonly Serializer _serializer;
        private static readonly Type PersistentRepresentationType = typeof(IPersistentRepresentation);

        private readonly List<string> _activeReplayTopics;
        public PulsarJournalExecutor(PulsarSettings settings, ILoggingAdapter log, Serializer serializer)
        {
            _activeReplayTopics = new List<string>();
            _log = log;
            _serializer = serializer; 
            _sqlClientOptions = new ClientOptions
            {
                Server = settings.ServiceUrl,
                Catalog = "pulsar",
                Schema = $"{settings.Tenant}/{settings.Namespace}"
            };
            _sql = PulsarSystem.NewSql();
        }
        public async ValueTask ReplayMessages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            //RETENTION POLICY MUST BE SENT AT THE NAMESPACE ELSE TOPIC IS DELETED
            var topic = $"journal".ToLower();
            _sqlClientOptions.Execute = $"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from {topic} WHERE PersistenceId = {persistenceId} AND SequenceNr BETWEEN {fromSequenceNr} AND {toSequenceNr} ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT {max}";
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
                var topic = $"journal";
                _sqlClientOptions.Execute = $"select SequenceNr as Id from {topic} WHERE PersistenceId = {persistenceId}  ORDER BY __sequence_id__ as DESC LIMIT 1";
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
        internal IPersistentRepresentation Deserialize(byte[] bytes)
        {
            return (IPersistentRepresentation)_serializer.FromBinary(bytes, PersistentRepresentationType);
        }
        public async IAsyncEnumerable<JournalEntry> ReplayTagged(ReplayTaggedMessages replay)
        {
            var topic = $"journal".ToLower();
            _sqlClientOptions.Execute = $"select Id, PersistenceId, SequenceNr, IsDeleted, Payload, Ordering, Tags from {topic} WHERE SequenceNr BETWEEN {replay.FromOffset} AND {replay.ToOffset} AND element_at(cast(json_parse(__properties__) as map(varchar, varchar)), '{replay.Tag.Trim().ToLower()}') = '{replay.Tag.Trim().ToLower()}' ORDER BY SequenceNr DESC, __publish_time__ DESC LIMIT {replay.Max}";
            _sql.SendQuery(new SqlQuery(_sqlClientOptions, e => { _log.Error(e.ToString()); }, l => { _log.Info(l); }));

            _sqlClientOptions.Execute = string.Empty;
            await foreach (var data in _sql.ReadResults(TimeSpan.FromSeconds(30)))
            {
                switch (data.Response)
                {
                    case DataResponse dr:
                        var entry = JsonSerializer.Deserialize<JournalEntry>(JsonSerializer.Serialize(dr.Data));
                        yield return entry;
                        break;
                    case StatsResponse sr:
                        if (_log.IsDebugEnabled)
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
        internal async ValueTask<long> GetMaxOrdering(ReplayTaggedMessages replay)
        {
            var topic = $"journal".ToLower();
            _sqlClientOptions.Execute = $"select Ordering from {topic} WHERE element_at(cast(json_parse(__properties__) as map(varchar, varchar)), '{replay.Tag.Trim().ToLower()}') = '{replay.Tag.Trim().ToLower()}' ORDER BY __publish_time__ DESC, Ordering DESC LIMIT 1";
            _sql.SendQuery(new SqlQuery(_sqlClientOptions, e => { _log.Error(e.ToString()); }, l => { _log.Info(l); }));

            _sqlClientOptions.Execute = string.Empty;
            var response = await _sql.ReadQueryResultAsync(TimeSpan.FromSeconds(30));
            var max = 0L;
            var data = response.Response;
            switch (data)
            {
                case DataResponse dr:
                    max = (long)dr.Data["Ordering"];
                    break;
                case StatsResponse sr:
                    _log.Info(JsonSerializer.Serialize(sr, new JsonSerializerOptions { WriteIndented = true }));
                    break;
                case ErrorResponse er:
                    throw new Exception(er.Error.FailureInfo.Message);
                default:
                    break;
            }
            return max;
        }
    }
}
