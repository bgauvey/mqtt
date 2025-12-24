using Xunit;
using MqttBridgeService.Services;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace MqttBridgeService.Tests.Services;

public class SequenceManagerTests
{
    [Fact]
    public async Task GetAndIncrementSequenceAsync_FirstCall_ReturnsZero()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act
        var result = await sequenceManager.GetAndIncrementSequenceAsync("node1");

        // Assert
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task GetAndIncrementSequenceAsync_MultipleCalls_ReturnsIncrementingSequence()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act
        var result1 = await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var result2 = await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var result3 = await sequenceManager.GetAndIncrementSequenceAsync("node1");

        // Assert
        Assert.Equal(0, result1);
        Assert.Equal(1, result2);
        Assert.Equal(2, result3);
    }

    [Fact]
    public async Task GetAndIncrementSequenceAsync_WrapAround_ReturnsZeroAfter255()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act: Set sequence to 255
        await sequenceManager.ResetSequenceAsync("node1");
        for (int i = 0; i < 255; i++)
        {
            await sequenceManager.GetAndIncrementSequenceAsync("node1");
        }

        var result255 = await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var result0 = await sequenceManager.GetAndIncrementSequenceAsync("node1");

        // Assert
        Assert.Equal(255, result255);
        Assert.Equal(0, result0);
    }

    [Fact]
    public async Task GetAndIncrementSequenceAsync_DifferentNodes_MaintainsSeparateSequences()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act
        var node1Result1 = await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var node2Result1 = await sequenceManager.GetAndIncrementSequenceAsync("node2");
        var node1Result2 = await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var node2Result2 = await sequenceManager.GetAndIncrementSequenceAsync("node2");

        // Assert
        Assert.Equal(0, node1Result1);
        Assert.Equal(0, node2Result1);
        Assert.Equal(1, node1Result2);
        Assert.Equal(1, node2Result2);
    }

    [Fact]
    public async Task ResetSequenceAsync_AfterIncrement_ResetsToZero()
    {
        // Arrange
        var sequenceManager = new SequenceManager();
        await sequenceManager.GetAndIncrementSequenceAsync("node1");
        await sequenceManager.GetAndIncrementSequenceAsync("node1");

        // Act
        await sequenceManager.ResetSequenceAsync("node1");
        var result = await sequenceManager.GetAndIncrementSequenceAsync("node1");

        // Assert
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task HasSequenceAsync_AfterGetAndIncrement_ReturnsTrue()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act
        await sequenceManager.GetAndIncrementSequenceAsync("node1");
        var result = await sequenceManager.HasSequenceAsync("node1");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task HasSequenceAsync_BeforeAnyOperation_ReturnsFalse()
    {
        // Arrange
        var sequenceManager = new SequenceManager();

        // Act
        var result = await sequenceManager.HasSequenceAsync("node1");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public async Task ClearAllAsync_RemovesAllSequences()
    {
        // Arrange
        var sequenceManager = new SequenceManager();
        await sequenceManager.GetAndIncrementSequenceAsync("node1");
        await sequenceManager.GetAndIncrementSequenceAsync("node2");

        // Act
        await sequenceManager.ClearAllAsync();

        // Assert
        Assert.False(await sequenceManager.HasSequenceAsync("node1"));
        Assert.False(await sequenceManager.HasSequenceAsync("node2"));
    }

    [Fact]
    public async Task GetAllNodeIdsAsync_ReturnsAllNodeIds()
    {
        // Arrange
        var sequenceManager = new SequenceManager();
        await sequenceManager.GetAndIncrementSequenceAsync("node1");
        await sequenceManager.GetAndIncrementSequenceAsync("node2");
        await sequenceManager.GetAndIncrementSequenceAsync("node3");

        // Act
        var nodeIds = await sequenceManager.GetAllNodeIdsAsync();

        // Assert
        Assert.Equal(3, nodeIds.Count);
        Assert.Contains("node1", nodeIds);
        Assert.Contains("node2", nodeIds);
        Assert.Contains("node3", nodeIds);
    }

    [Fact]
    public async Task GetAndIncrementSequenceAsync_ConcurrentCalls_NoCollisions()
    {
        // Arrange
        var sequenceManager = new SequenceManager();
        var tasks = new List<Task<byte>>();

        // Act: Run 100 concurrent calls
        for (int i = 0; i < 100; i++)
        {
            tasks.Add(sequenceManager.GetAndIncrementSequenceAsync("node1"));
        }

        var results = await Task.WhenAll(tasks);

        // Assert: All values should be unique
        var uniqueResults = results.Distinct().ToList();
        Assert.Equal(100, uniqueResults.Count);
    }
}
