using System;
using System.Collections.Generic;

using KPLNET.Http;
using KPLNET.Utils;
using KPLNET.Metrics;
using KPLNETInterface;
using KPLNET.Core.Clients.KinesisClient;

namespace KPLNET.Kinesis.Core
{
    using UserRecordCallback = Action<UserRecord>;
    using ShardMapInvalidateCallback = Action<DateTime>;
    using ErrorCallback = Action<string, string>;

    public class MetricsPutter
    {
        private MetricsManager metrics_manager;
        private string stream;

        public MetricsPutter(MetricsManager metrics_manager, AwsHttpResult result)
        {
            this.metrics_manager = metrics_manager;
            stream = result.context<PutRecordsRequest>().stream();
        }

        public MetricsPutter put(
            string name,
            double val,
            ulong shard_id = 0,
            string err_code = null)
        {
            var f = metrics_manager.finder().set_name(name);
            if (!string.IsNullOrEmpty(err_code))
            {
                f.set_error_code(err_code);
            }
            f.set_stream(stream);
            if (shard_id != 0)
            {
                f.set_shard(ShardMap.shard_id_to_str(shard_id));
            }
            f.find().Put(val);
            return this;
        }
    }

    public class Retrier
    {
        private KPLNETConfiguration config_;
        private UserRecordCallback finish_cb_;
        private UserRecordCallback retry_cb_;
        private ShardMapInvalidateCallback shard_map_invalidate_cb_;
        private ErrorCallback error_cb_;
        private MetricsManager metrics_manager_;

        public Retrier(KPLNETConfiguration config, UserRecordCallback finish_cb, UserRecordCallback retry_cb, ShardMapInvalidateCallback shard_map_invalidate_cb, ErrorCallback error_cb = null, MetricsManager metrics_manager = null)
        {
            config_ = config;
            finish_cb_ = finish_cb;
            retry_cb_ = retry_cb;
            shard_map_invalidate_cb_ = shard_map_invalidate_cb;
            error_cb_ = error_cb;
            metrics_manager_ = metrics_manager;

            if (metrics_manager == null)
                metrics_manager = new NullMetricsManager();
            //if (error_cb == null)
            //    error_cb = new ErrorCallback();
        }

        #region Aws Kinesis Response
        public void put(AwsKinesisResult result)
        {
            handle_put_records_result(result);
        }

        void handle_put_records_result(AwsKinesisResult result)
        {
            //emit_metrics(result);

            try
            {
                if (result.successful())
                {
                    var status_code = result.status_code();
                    if ((int)status_code == 200)
                    {
                        on_200(result);
                    }
                    else if ((int)status_code >= 500 && (int)status_code < 600)
                    {
                        retry_not_expired(result);
                    }
                    else
                    {
                        // For PutRecords, errors that apply to individual kinesis records
                        // (like throttling, too big or bad format) come back in code 200s.
                        // This is different from plain old PutRecord, where those come back
                        // with code 400. As such, all the errors we want to retry on are
                        // handled in the 200 case. All 400 codes are therefore not retryable.
                        StdErrorOut.Instance.StdError(string.Format("PutRecords failed: {0}", result.response().PutRecordsResponse.ToString()));
                        fail(result);
                    }
                }
                else
                {
                    retry_not_expired(result);
                }
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError(string.Format("Unexpected error encountered processing http result: {0}", ex.ToString()));
                fail(result, "Unexpected Error", ex.ToString());
            }
        }

        public void put(KinesisRecord kr, string err_code, string err_msg)
        {
            var now = DateTime.Now;
            retry_not_expired(kr, now, now, err_code, err_msg);
        }

        void on_200(AwsKinesisResult result)
        {
            var response = result.response().PutRecordsResponse;
            var records = response.Records;
            var prr = result.context<PutRecordsRequest>();
            //MetricsPutter metrics_putter = new MetricsPutter(metrics_manager_, result);

            // If somehow there's a size mismatch, subsequent code may crash from
            // array out of bounds, so we're going to explicitly catch it here and
            // print a nicer message. Also, if there's a size mismatch, we can no longer
            // be sure which result is for which record, so we better fail all of them.
            // None of this is expected to happen if the backend behaves correctly,
            // but if it does happen, this will make it easier to identify the problem.
            if (records.Count != (int)prr.Size())
            {
                string ss = "Count of records in PutRecords response differs from the number " + "sent: " + records.Count + " received, but " + prr.Size() + " were sent.";
                StdErrorOut.Instance.StdError(ss);
                StdErrorOut.Instance.StdOut(LogLevel.debug, ss.ToString());
                fail(result, "Record Count Mismatch", ss);
                return;
            }

            for (int i = 0; i < (int)prr.Size(); i++)
            {
                var record = records[i];
                var kr = prr.Items()[i];
                bool success = !string.IsNullOrEmpty(record.SequenceNumber);
                var start = result.start_time();
                var end = result.end_time();

                var shard_id = kr.Items()[0].Predicted_shard();
                //if (success)
                //{
                //    metrics_putter.put
                //        (Names.KinesisRecordsPut, 1, (ulong)shard_id).put
                //        (Names.KinesisRecordsDataPut, kr.accurate_size(), (ulong)shard_id).put
                //        (Names.AllErrors, 0, (ulong)shard_id);
                //}
                //else
                //{
                //    metrics_putter.put
                //        (Names.KinesisRecordsPut, 0, (ulong)shard_id).put
                //        (Names.ErrorsByCode, 1, (ulong)shard_id, record["ErrorCode"].ToString()).put
                //        (Names.AllErrors, 1, shard_id);
                //}

                if (success)
                    foreach (var ur in kr.Items())
                        succeed_if_correct_shard(ur, start, end, record.ShardId, record.SequenceNumber);
                else
                {
                    string err_code = record.ErrorCode;
                    string err_msg = record.ErrorMessage;

                    bool can_retry = (!config_.failIfThrottled && err_code == "ProvisionedThroughputExceededException") || (err_code == "InternalFailure") || (err_code == "ServiceUnavailable");

                    if (can_retry)
                        retry_not_expired(kr, start, end, err_code, err_msg);
                    else
                        fail(kr, start, end, err_code, err_msg);
                }
            }
        }

        void fail(AwsKinesisResult result)
        {
            fail(result,
                 result.status_code().ToString(),
                 result.error());
        }

        void fail(AwsKinesisResult result, string err_code, string err_msg)
        {
            var response = result.response().PutRecordsResponse;
            var records = result.context<PutRecordsRequest>().Items();
            if (response != null && response.Records != null && response.Records.Count == records.Count)
            {
                for (int i = 0; i < records.Count; i++)
                    fail(records[i], result.start_time(), result.end_time(), response.Records[i].ErrorCode, response.Records[i].ErrorMessage);
            }
            else
            {
                foreach (var kr in records)
                    fail(kr, result.start_time(), result.end_time(), err_code, err_msg);
            }
        }

        void retry_not_expired(AwsKinesisResult result)
        {
            retry_not_expired
              (
                result,
                result.status_code().ToString(),
                result.error()
              );
        }

        void retry_not_expired(AwsKinesisResult result, string err_code, string err_msg)
        {
            foreach (var kr in result.context<PutRecordsRequest>().Items())
                retry_not_expired(kr, result.start_time(), result.end_time(), err_code, err_msg);
        }

        #endregion

        #region AwsSocketClientResponse

        void handle_put_records_result(AwsHttpResult result)
        {
            emit_metrics(result);

            try
            {
                if (result.successful())
                {
                    var status_code = result.status_code();
                    if (status_code == 200)
                    {
                        on_200(result);
                    }
                    else if (status_code >= 500 && status_code < 600)
                    {
                        retry_not_expired(result);
                    }
                    else
                    {
                        // For PutRecords, errors that apply to individual kinesis records
                        // (like throttling, too big or bad format) come back in code 200s.
                        // This is different from plain old PutRecord, where those come back
                        // with code 400. As such, all the errors we want to retry on are
                        // handled in the 200 case. All 400 codes are therefore not retryable.
                        StdErrorOut.Instance.StdError(string.Format("PutRecords failed: {0}", result.response_body()));
                        fail(result);
                    }
                }
                else
                {
                    retry_not_expired(result);
                }
            }
            catch (Exception ex)
            {
                StdErrorOut.Instance.StdError(string.Format("Unexpected error encountered processing http result: {0}", ex.ToString()));
                fail(result, "Unexpected Error", ex.ToString());
            }
        }

        public void put(AwsHttpResult result)
        {
            handle_put_records_result(result);
        }

        void on_200(AwsHttpResult result)
        {
            dynamic json = System.Web.Helpers.Json.Decode(result.response_body());
            List<dynamic> records = json["Records"];
            var prr = result.context<PutRecordsRequest>();
            MetricsPutter metrics_putter = new MetricsPutter(metrics_manager_, result);

            // If somehow there's a size mismatch, subsequent code may crash from
            // array out of bounds, so we're going to explicitly catch it here and
            // print a nicer message. Also, if there's a size mismatch, we can no longer
            // be sure which result is for which record, so we better fail all of them.
            // None of this is expected to happen if the backend behaves correctly,
            // but if it does happen, this will make it easier to identify the problem.
            if (records.Count != (int)prr.Size())
            {
                string ss = "Count of records in PutRecords response differs from the number " + "sent: " + records.Count + " received, but " + prr.Size() + " were sent.";
                StdErrorOut.Instance.StdError(ss);
                fail(result, "Record Count Mismatch", ss);
                return;
            }

            for (int i = 0; i < (int)prr.Size(); i++)
            {
                var record = records[i];
                var kr = prr.Items()[i];
                bool success = record["SequenceNumber"];
                var start = result.start_time();
                var end = result.end_time();

                var shard_id = kr.Items()[0].Predicted_shard();
                if (success)
                {
                    metrics_putter.put
                        (Names.KinesisRecordsPut, 1, (ulong)shard_id).put
                        (Names.KinesisRecordsDataPut, kr.accurate_size(), (ulong)shard_id).put
                        (Names.AllErrors, 0, (ulong)shard_id);
                }
                else
                {
                    metrics_putter.put
                        (Names.KinesisRecordsPut, 0, (ulong)shard_id).put
                        (Names.ErrorsByCode, 1, (ulong)shard_id, record["ErrorCode"].ToString()).put
                        (Names.AllErrors, 1, shard_id);
                }

                if (success)
                    foreach (var ur in kr.Items())
                        succeed_if_correct_shard(ur, start, end, record["ShardId"], record["SequenceNumber"]);
                else
                {
                    string err_code = record["ErrorCode"];
                    string err_msg = record["ErrorMessage"];

                    bool can_retry = (!config_.failIfThrottled && err_code == "ProvisionedThroughputExceededException") || (err_code == "InternalFailure") || (err_code == "ServiceUnavailable");

                    if (can_retry)
                        retry_not_expired(kr, start, end, err_code, err_msg);
                    else
                        fail(kr, start, end, err_code, err_msg);
                }
            }
        }

        void retry_not_expired(AwsHttpResult result)
        {
            retry_not_expired
              (
                result,
                result.successful() ? result.status_code().ToString() : "Exception",
                result.successful() ? result.response_body().Substring(0, 4096) : result.error()
              );
        }

        void retry_not_expired(AwsHttpResult result, string err_code, string err_msg)
        {
            foreach (var kr in result.context<PutRecordsRequest>().Items())
                retry_not_expired(kr, result.start_time(), result.end_time(), err_code, err_msg);
        }

        void fail(AwsHttpResult result)
        {
            fail(result,
                 result.successful() ? result.status_code().ToString() : "Exception",
                 result.successful() ? result.response_body().Substring(0, 4096) : result.error());
        }

        void fail(AwsHttpResult result, string err_code, string err_msg)
        {
            foreach (var kr in result.context<PutRecordsRequest>().Items())
            {
                fail(kr, result.start_time(), result.end_time(), err_code, err_msg);
            }
        }

        void emit_metrics(AwsHttpResult result)
        {
            MetricsPutter metrics_putter = new MetricsPutter(metrics_manager_, result);
            PutRecordsRequest prr = result.context<PutRecordsRequest>();

            double num_urs = 0;
            foreach (var kr in prr.Items())
            {
                metrics_putter.put(Names.UserRecordsPerKinesisRecord, kr.Items().Count, (ulong)kr.Items()[kr.Items().Count - 1].Predicted_shard());
                num_urs += kr.Items().Count;
            }

            metrics_putter.put
                (Names.RequestTime, result.duration_millis()).put
                (Names.KinesisRecordsPerPutRecordsRequest, prr.Items().Count).put
                (Names.UserRecordsPerPutRecordsRequest, num_urs);

            string err_code = null;
            if (result.successful())
            {
                var status_code = result.status_code();
                if (status_code != 200)
                {
                    // TODO parse the json (if any) to get the error code
                    err_code = "Http" + status_code;
                }
            }
            else
            {
                err_code = result.error().Substring(0, 255);
            }

            if (err_code != null)
            {
                metrics_putter.put
                    (Names.ErrorsByCode, 1, 0, err_code).put
                    (Names.AllErrors, 1);
            }
        }

        #endregion

        void retry_not_expired(KinesisRecord kr, DateTime start, DateTime end, string err_code, string err_msg)
        {
            foreach (var ur in kr.Items())
                retry_not_expired(ur, start, end, err_code, err_msg);
        }

        void retry_not_expired(UserRecord ur, DateTime start, DateTime end, string err_code, string err_msg)
        {
            ur.add_attempt(new Attempt().set_start(start).set_end(end).set_error(err_code, err_msg));

            if (ur.expired())
                fail(ur, DateTime.Now, DateTime.Now, "Expired", "Record has reached expiration");
            else
            {
                // TimeSensitive automatically sets the deadline to the expiration if the given deadline is later than the expiration.
                ur.set_deadline_from_now((long)config_.recordMaxBufferedTime / 2);
                retry_cb_(ur);
            }
        }

        void fail(KinesisRecord kr, DateTime start, DateTime end, string err_code, string err_msg)
        {
            foreach (var ur in kr.Items())
                fail(ur, start, end, err_code, err_msg);
        }

        public void fail(UserRecord ur, DateTime start, DateTime end, string err_code, string err_msg)
        {
            finish_user_record(ur, new Attempt().set_start(start).set_end(end).set_error(err_code, err_msg));
        }

        public void succeed_if_correct_shard(UserRecord ur, DateTime start, DateTime end, string shard_id, string sequence_number)
        {
            StdErrorOut.Instance.StdOut(LogLevel.debug, "succeed_if_correct_shard");
            if (ur.Predicted_shard() != -1 && ur.Predicted_shard() != ShardMap.shard_id_from_str(shard_id))
            {
                StdErrorOut.Instance.StdOut(LogLevel.warn, string.Format("Record went to shard {0} instead of the prediceted shard {1}; this  usually means the sharp map has changed.", shard_id, ur.Predicted_shard()));
                shard_map_invalidate_cb_(start);

                retry_not_expired(ur, start, end, "Wrong Shard", "Record did not end up in expected shard.");
            }
            else
            {
                finish_user_record(ur, new Attempt().set_start(start).set_end(end).set_result(shard_id, sequence_number));
            }
        }

        void finish_user_record(UserRecord ur, Attempt final_attempt)
        {
            if (!ur.Finished())
            {
                StdErrorOut.Instance.StdOut(LogLevel.debug, "finish_user_record -> if (!ur.Finished())");
                ur.add_attempt(final_attempt);
                //emit_metrics(ur);
                StdErrorOut.Instance.StdOut(LogLevel.debug, "finish_cb_(ur)");
                finish_cb_(ur);
            }
        }

        void emit_metrics(UserRecord ur)
        {
            if (ur.Attempts().Count == 0)
                return;

            bool successful = ur.Attempts()[ur.Attempts().Count - 1].Success();
            string shard_id = "";
            if (successful)
            {
                shard_id = ur.Attempts()[ur.Attempts().Count - 1].Shard_id();
            }
            else if (ur.Predicted_shard() != 0)
            {
                shard_id = ShardMap.shard_id_to_str((ulong)ur.Predicted_shard());
            }

            Action<string, double> put = (name, val) =>
            {
                var f = metrics_manager_.finder().set_name(name).set_stream(ur.Stream());
                if (!string.IsNullOrEmpty(shard_id))
                {
                    f.set_shard(shard_id);
                }
                f.find().Put(val);
            };

            if (successful)
            {
                put(Names.UserRecordsPut, 1);
                put(Names.UserRecordsDataPut, ur.Data().Length);
            }
            else
            {
                put(Names.UserRecordsPut, 0);
                if (ur.Attempts()[ur.Attempts().Count - 1].Error_code() == "Expired")
                {
                    put(Names.UserRecordExpired, 1);
                }
            }

            var last = ur.Arrival();
            foreach (var a in ur.Attempts())
            {
                put(Names.BufferingTime, (a.Start() - last).TotalMilliseconds);
                last = a.End();
            }

            put(Names.RetriesPerRecord, ur.Attempts().Count - 1);
        }

    }
}
