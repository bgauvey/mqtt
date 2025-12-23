using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace MqttBridgeService.Services;

public class SequenceManager
{
    private readonly Dictionary<string, byte> _sequenceNumbers = new();
    private readonly SemaphoreSlim _lock = new(1, 1);

    public byte GetAndIncrementSequence(string nodeId)
    {
        _lock.Wait();
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

    public void ResetSequence(string nodeId)
    {
        _lock.Wait();
        try
        {
            _sequenceNumbers[nodeId] = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    public bool HasSequence(string nodeId)
    {
        _lock.Wait();
        try
        {
            return _sequenceNumbers.ContainsKey(nodeId);
        }
        finally
        {
            _lock.Release();
        }
    }

    public void ClearAll()
    {
        _lock.Wait();
        try
        {
            _sequenceNumbers.Clear();
        }
        finally
        {
            _lock.Release();
        }
    }

    public List<string> GetAllNodeIds()
    {
        _lock.Wait();
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
