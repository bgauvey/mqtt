using Xunit;
using MqttBridgeService.Services;
using MqttBridgeService.Models;

namespace MqttBridgeService.Tests.Services;

public class SparkplugServiceTests
{
    private readonly SparkplugService _sparkplugService;

    public SparkplugServiceTests()
    {
        _sparkplugService = new SparkplugService();
    }

    [Theory]
    [InlineData("spBv1.0/GroupA/NBIRTH/Node1", "GroupA", "Node1", null, "NBIRTH", true)]
    [InlineData("spBv1.0/GroupA/NDEATH/Node1", "GroupA", "Node1", null, "NDEATH", true)]
    [InlineData("spBv1.0/GroupA/NDATA/Node1", "GroupA", "Node1", null, "NDATA", true)]
    [InlineData("spBv1.0/GroupA/DBIRTH/Node1/Device1", "GroupA", "Node1", "Device1", "DBIRTH", true)]
    [InlineData("spBv1.0/GroupA/DDEATH/Node1/Device1", "GroupA", "Node1", "Device1", "DDEATH", true)]
    [InlineData("spBv1.0/GroupA/DDATA/Node1/Device1", "GroupA", "Node1", "Device1", "DDATA", true)]
    [InlineData("", null, null, null, null, false)]
    [InlineData("invalid/topic", null, null, null, null, false)]
    [InlineData("spBv1.0/GroupA", null, null, null, null, false)]
    [InlineData("spBv1.0/GroupA/INVALID/Node1", null, null, null, null, false)]
    [InlineData("spBv2.0/GroupA/DDATA/Node1/Device1", null, null, null, null, false)]
    public void ParseTopic_VariousTopics_ReturnsExpectedResult(
        string topic,
        string? expectedGroupId,
        string? expectedNodeId,
        string? expectedDeviceName,
        string? expectedMessageType,
        bool expectedIsValid)
    {
        // Act
        var result = _sparkplugService.ParseTopic(topic);

        // Assert
        Assert.Equal(expectedIsValid, result.IsValid);
        Assert.Equal(expectedGroupId, result.GroupId);
        Assert.Equal(expectedNodeId, result.NodeId);
        Assert.Equal(expectedDeviceName, result.DeviceName);
        Assert.Equal(expectedMessageType, result.MessageType);
    }

    [Fact]
    public void ParseTopic_NullTopic_ReturnsInvalidTopicInfo()
    {
        // Act
        var result = _sparkplugService.ParseTopic(null!);

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ParseTopic_WhitespaceTopic_ReturnsInvalidTopicInfo()
    {
        // Act
        var result = _sparkplugService.ParseTopic("   ");

        // Assert
        Assert.False(result.IsValid);
    }

    [Fact]
    public void ConvertJsonToMetrics_ValidJson_ReturnsMetrics()
    {
        // Arrange
        var json = @"{""temperature"": 25, ""humidity"": 60.5, ""active"": true, ""status"": ""online""}";

        // Act
        var metrics = _sparkplugService.ConvertJsonToMetrics(json);

        // Assert
        Assert.Equal(4, metrics.Count);
        Assert.Contains(metrics, m => m.Name == "temperature");
        Assert.Contains(metrics, m => m.Name == "humidity");
        Assert.Contains(metrics, m => m.Name == "active");
        Assert.Contains(metrics, m => m.Name == "status");
    }

    [Fact]
    public void ExtractMetricDefinitionsFromJson_ValidJson_ReturnsMetricDefinitions()
    {
        // Arrange
        var json = @"{""temperature"": 25, ""humidity"": 60.5}";

        // Act
        var metrics = _sparkplugService.ExtractMetricDefinitionsFromJson(json);

        // Assert
        Assert.Equal(2, metrics.Count);
        Assert.Contains(metrics, m => m.Name == "temperature");
        Assert.Contains(metrics, m => m.Name == "humidity");
    }

    [Fact]
    public void SerializePayload_ValidMetrics_ReturnsValidBytes()
    {
        // Arrange
        var json = @"{""temperature"": 25}";
        var metrics = _sparkplugService.ConvertJsonToMetrics(json);
        byte sequenceNumber = 5;

        // Act
        var payloadBytes = _sparkplugService.SerializePayload(metrics, sequenceNumber);

        // Assert
        Assert.NotNull(payloadBytes);
        Assert.NotEmpty(payloadBytes);
    }
}
