using System;
using System.Collections.Generic;
using Google.Protobuf;
using MqttBridgeService.Models;
using Newtonsoft.Json.Linq;
using SparkplugMetric = SparkplugNet.VersionB.Data.Metric;
using SparkplugDataType = SparkplugNet.VersionB.Data.DataType;
using ProtoPayload = Org.Eclipse.Tahu.Protobuf.Payload;

namespace MqttBridgeService.Services;

public class SparkplugService
{
    public TopicInfo ParseTopic(string topic)
    {
        // Expected format: spBv1.0/{group}/DDATA/{edgeNode}/{deviceName}
        var parts = topic.Split('/');
        if (parts.Length >= 4 && parts[2] == "DDATA")
        {
            return new TopicInfo
            {
                GroupId = parts.Length >= 2 ? parts[1] : null,
                NodeId = parts[3],
                DeviceName = parts.Length >= 5 ? parts[4] : null
            };
        }
        return new TopicInfo();
    }

    public List<SparkplugMetric> ConvertJsonToMetrics(string jsonPayload)
    {
        var metrics = new List<SparkplugMetric>();
        var json = JObject.Parse(jsonPayload);

        foreach (var property in json.Properties())
        {
            var metric = CreateMetricFromProperty(property, includeValue: true);
            metrics.Add(metric);
        }

        return metrics;
    }

    public List<SparkplugMetric> ExtractMetricDefinitionsFromJson(string jsonPayload)
    {
        var metrics = new List<SparkplugMetric>();
        var json = JObject.Parse(jsonPayload);

        foreach (var property in json.Properties())
        {
            var metric = CreateMetricFromProperty(property, includeValue: false);
            metrics.Add(metric);
        }

        return metrics;
    }

    public byte[] SerializePayload(List<SparkplugMetric> metrics, byte sequenceNumber)
    {
        var protoPayload = new ProtoPayload
        {
            Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Seq = sequenceNumber
        };

        foreach (var metric in metrics)
        {
            var protoMetric = new ProtoPayload.Types.Metric
            {
                Name = metric.Name,
                Timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Datatype = (uint)metric.DataType
            };

            SetMetricValue(protoMetric, metric);
            protoPayload.Metrics.Add(protoMetric);
        }

        return protoPayload.ToByteArray();
    }

    private SparkplugMetric CreateMetricFromProperty(JProperty property, bool includeValue)
    {
        SparkplugMetric metric;
        var timestamp = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        switch (property.Value.Type)
        {
            case JTokenType.Integer:
                metric = new SparkplugMetric(
                    property.Name,
                    SparkplugDataType.Int32,
                    includeValue ? property.Value.Value<int>() : 0)
                {
                    Timestamp = timestamp
                };
                break;
            case JTokenType.Float:
                metric = new SparkplugMetric(
                    property.Name,
                    SparkplugDataType.Double,
                    includeValue ? property.Value.Value<double>() : 0.0)
                {
                    Timestamp = timestamp
                };
                break;
            case JTokenType.Boolean:
                metric = new SparkplugMetric(
                    property.Name,
                    SparkplugDataType.Boolean,
                    includeValue ? property.Value.Value<bool>() : false)
                {
                    Timestamp = timestamp
                };
                break;
            case JTokenType.String:
            default:
                metric = new SparkplugMetric(
                    property.Name,
                    SparkplugDataType.String,
                    includeValue ? (property.Value.Value<string>() ?? string.Empty) : string.Empty)
                {
                    Timestamp = timestamp
                };
                break;
        }

        return metric;
    }

    private void SetMetricValue(ProtoPayload.Types.Metric protoMetric, SparkplugMetric metric)
    {
        switch (metric.DataType)
        {
            case SparkplugDataType.Int32:
                protoMetric.IntValue = metric.Value != null ? Convert.ToUInt32(metric.Value) : 0;
                break;
            case SparkplugDataType.String:
                protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                break;
            case SparkplugDataType.Boolean:
                protoMetric.BooleanValue = metric.Value != null && Convert.ToBoolean(metric.Value);
                break;
            case SparkplugDataType.Double:
                protoMetric.DoubleValue = metric.Value != null ? Convert.ToDouble(metric.Value) : 0.0;
                break;
            case SparkplugDataType.Float:
                protoMetric.FloatValue = metric.Value != null ? Convert.ToSingle(metric.Value) : 0.0f;
                break;
            default:
                protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                break;
        }
    }
}
