using System;
using System.Collections.Generic;
using System.Linq;
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
        // Expected format: spBv1.0/{group}/{messageType}/{edgeNode}/{deviceName}
        // Valid message types: NBIRTH, NDEATH, DBIRTH, DDEATH, NDATA, DDATA
        if (string.IsNullOrWhiteSpace(topic))
        {
            return new TopicInfo();
        }

        var parts = topic.Split('/');

        // Minimum required: spBv1.0/{group}/{messageType}/{edgeNode}
        if (parts.Length < 4)
        {
            return new TopicInfo();
        }

        // Validate namespace (should be spBv1.0)
        if (parts[0] != "spBv1.0")
        {
            return new TopicInfo();
        }

        var messageType = parts[2];
        var validMessageTypes = new[] { "NBIRTH", "NDEATH", "DBIRTH", "DDEATH", "NDATA", "DDATA" };

        if (!validMessageTypes.Contains(messageType))
        {
            return new TopicInfo();
        }

        var topicInfo = new TopicInfo
        {
            GroupId = parts[1],
            NodeId = parts[3],
            MessageType = messageType
        };

        // Device name is only present for D* messages (DBIRTH, DDEATH, DDATA)
        if (messageType.StartsWith("D") && parts.Length >= 5)
        {
            topicInfo.DeviceName = parts[4];
        }

        return topicInfo;
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
                if (metric.Value != null)
                {
                    try
                    {
                        var intValue = Convert.ToInt32(metric.Value);
                        protoMetric.IntValue = intValue >= 0 ? (uint)intValue : 0;
                    }
                    catch (OverflowException)
                    {
                        protoMetric.IntValue = 0;
                    }
                    catch (FormatException)
                    {
                        protoMetric.IntValue = 0;
                    }
                }
                else
                {
                    protoMetric.IntValue = 0;
                }
                break;
            case SparkplugDataType.String:
                protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                break;
            case SparkplugDataType.Boolean:
                protoMetric.BooleanValue = metric.Value != null && Convert.ToBoolean(metric.Value);
                break;
            case SparkplugDataType.Double:
                if (metric.Value != null)
                {
                    try
                    {
                        protoMetric.DoubleValue = Convert.ToDouble(metric.Value);
                    }
                    catch (OverflowException)
                    {
                        protoMetric.DoubleValue = 0.0;
                    }
                    catch (FormatException)
                    {
                        protoMetric.DoubleValue = 0.0;
                    }
                }
                else
                {
                    protoMetric.DoubleValue = 0.0;
                }
                break;
            case SparkplugDataType.Float:
                if (metric.Value != null)
                {
                    try
                    {
                        protoMetric.FloatValue = Convert.ToSingle(metric.Value);
                    }
                    catch (OverflowException)
                    {
                        protoMetric.FloatValue = 0.0f;
                    }
                    catch (FormatException)
                    {
                        protoMetric.FloatValue = 0.0f;
                    }
                }
                else
                {
                    protoMetric.FloatValue = 0.0f;
                }
                break;
            default:
                protoMetric.StringValue = metric.Value?.ToString() ?? string.Empty;
                break;
        }
    }
}
