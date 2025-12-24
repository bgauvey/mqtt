using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MqttBridgeService.Services;

public class SequenceManager
{
    private readonly Dictionary<string, byte> _sequenceNumbers = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public async Task<byte> GetAndIncrementSequenceAsync(string nodeId)
    {
        await _lock.WaitAsync();
        try
        {
            if (!_sequenceNumbers.TryGetValue(nodeId, out var seq))
            {
                seq = 0;
                _sequenceNumbers[nodeId] = seq;
            }
            var current = seq;
            _sequenceNumbers[nodeId] = (byte)((seq + 1) % 256);
            return current;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ResetSequenceAsync(string nodeId)
    {
        await _lock.WaitAsync();
        try
        {
            _sequenceNumbers[nodeId] = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<bool> HasSequenceAsync(string nodeId)
    {
        await _lock.WaitAsync();
        try
        {
            return _sequenceNumbers.ContainsKey(nodeId);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task ClearAllAsync()
    {
        await _lock.WaitAsync();
        try
        {
            _sequenceNumbers.Clear();
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<List<string>> GetAllNodeIdsAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _sequenceNumbers.Keys.ToList();
        }
        finally
        {
            _lock.Release();
        }
    }
}
