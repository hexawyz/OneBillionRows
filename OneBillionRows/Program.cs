using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

long startTimestamp = Stopwatch.GetTimestamp();

using var file = File.OpenHandle(args[0], FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan, 0);
nint length = (nint)RandomAccess.GetLength(file);
using var mapping = MemoryMappedFile.CreateFromFile(file, null, length, MemoryMappedFileAccess.Read, HandleInheritability.None, true);
using var accessor = mapping.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

unsafe nint GetPointer(SafeBuffer buffer)
{
	byte* ptr = null;
	buffer.AcquirePointer(ref ptr);
	return (nint)ptr;
}

nint startPointer = GetPointer(accessor.SafeMemoryMappedViewHandle);

(string Name, ItemData Data)[] items;
try
{
#if RUNNER_ST
	var dictionary = new SingleThreadedRunner(startPointer, length).Process();
#else
	var dictionary = await new MultiThreadedRunner(startPointer, length).ProcessAsync();
#endif

	items = new (string, ItemData)[dictionary.Count];
	int i = 0;
	foreach (var kvp in dictionary)
	{
		items[i++] = (kvp.Key.ToString(), kvp.Value);
	}

	Array.Sort(items, (x, y) => StringComparer.Ordinal.Compare(x.Name, y.Name));
}
finally
{
	accessor.SafeMemoryMappedViewHandle.ReleasePointer();
}
Console.Write("{");
foreach (var item in items)
{
	Console.Write($"{item.Name}={Parser.FormatFixedPoint(item.Data.Min)}/{Parser.FormatFixedPoint(item.Data.Sum / item.Data.Count)}/{Parser.FormatFixedPoint(item.Data.Max)}, ");
}
Console.WriteLine("}");

Console.WriteLine(Stopwatch.GetElapsedTime(startTimestamp));

static class Parser
{
	// This is the core algorithm. It works on a single thread without problems.
	public static unsafe void Parse(Dictionary<Utf8StringRef, ItemData> aggregates, ref byte start, ref byte end)
	{
		ref byte current = ref start;
		// Use two nested loops so that we can keep working on a large window of without having to update it every time.
		while (Unsafe.IsAddressLessThan(ref current, ref end))
		{
			const nint WindowSize = 0x40000080;
			const int Threshold = 0x80;

			// We determine the window size here
			nint remainingLength = Unsafe.ByteOffset(ref current, ref end);
			ref byte windowEnd = ref Unsafe.AddByteOffset(ref current, nint.Min(WindowSize + Threshold, remainingLength));
			ref byte windowRenewalThreshold = ref remainingLength > WindowSize ? ref Unsafe.SubtractByteOffset(ref windowEnd, Threshold) : ref windowEnd;

			// Parse repeatedly until the threshold is crossed.
			do
			{
				int semicolonIndex = MemoryMarshal.CreateSpan(ref current, (int)Unsafe.ByteOffset(ref current, ref windowEnd)).IndexOf((byte)';');
				ref var itemData = ref CollectionsMarshal.GetValueRefOrAddDefault(aggregates, new Utf8StringRef((byte*)Unsafe.AsPointer(ref current), semicolonIndex), out bool exists);
				ref byte numberStart = ref Unsafe.Add(ref current, semicolonIndex + 1);
				int rowEndIndex = MemoryMarshal.CreateSpan(ref numberStart, (int)Unsafe.ByteOffset(ref numberStart, ref windowEnd)).IndexOf((byte)'\n');
				int value = ParseFixedPoint(ref numberStart, ref Unsafe.Add(ref numberStart, rowEndIndex));
				current = ref Unsafe.Add(ref numberStart, rowEndIndex + 1);

				if (exists)
				{
					itemData.Aggregate(value);
				}
				else
				{
					itemData = new(value);
				}

			}
			while (Unsafe.IsAddressLessThan(ref current, ref windowRenewalThreshold));
		}
	}

	// Hacky parsing that will do no validation.
	private static int ParseFixedPoint(ref byte start, ref byte end)
	{
		bool isNegative = start == (byte)'-';
		ref byte current = ref isNegative ? ref Unsafe.Add(ref start, 1) : ref start;
		end = ref (end == '\r' ? ref Unsafe.Subtract(ref end, 1) : ref end);
		int value = current - '0';
		current = ref Unsafe.Add(ref current, 1);

		while (!Unsafe.IsAddressGreaterThan(ref current, ref end))
		{
			int c = current - '0';
			value = c >= 0 ? 10 * value + c : value;
			current = ref Unsafe.Add(ref current, 1);
		}

		return isNegative ? -value : value;
	}

	public static string FormatFixedPoint(int value)
	{
		if ((uint)value < 10)
		{
			return value switch
			{
				0 => "0.0",
				1 => "0.1",
				2 => "0.2",
				3 => "0.3",
				4 => "0.4",
				5 => "0.5",
				6 => "0.6",
				7 => "0.7",
				8 => "0.8",
				9 => "0.9",
				_ => throw new InvalidOperationException(),
			};
		}
		else if (value < 0 && value > -10)
		{
			return -value switch
			{
				0 => "-0.0",
				1 => "-0.1",
				2 => "-0.2",
				3 => "-0.3",
				4 => "-0.4",
				5 => "-0.5",
				6 => "-0.6",
				7 => "-0.7",
				8 => "-0.8",
				9 => "-0.9",
				_ => throw new InvalidOperationException(),
			};
		}

		Span<char> buffer = stackalloc char[12];

		value.TryFormat(buffer, out int count, default, CultureInfo.InvariantCulture);

		buffer[count] = buffer[count - 1];
		buffer[count - 1] = '.';

		return buffer[..(count + 1)].ToString();
	}
}

#if RUNNER_ST
unsafe sealed class SingleThreadedRunner
{
	private readonly byte* _start;
	private readonly byte* _end;

	public SingleThreadedRunner(nint start, nint length)
	{
		_start = (byte*)start;
		_end = (byte*)(start + length);
	}

	public Dictionary<Utf8StringRef, ItemData> Process()
	{
		var dictionary = new Dictionary<Utf8StringRef, ItemData>();
		Parser.Parse(dictionary, ref Unsafe.AsRef<byte>(_start), ref Unsafe.AsRef<byte>(_end));
		return dictionary;
	}
}
#else
sealed class MultiThreadedRunner
{
	private nint _nextChunkStartPointer;
	private readonly nint _endPointer;
	private readonly int _chunkSize;
	private readonly int _taskCount;

	private const int MinChunkSize = 1024;
	private const int MaxChunkSize = 1 * 1024 * 1024;
	private const int ChunkExtraSizeTolerance = 256;

	public MultiThreadedRunner(nint start, nint length)
	{
		_nextChunkStartPointer = start;
		_endPointer = start + length;
		nint chunkSize = Math.Min(Math.Max(MinChunkSize, length / Environment.ProcessorCount), MaxChunkSize);
		_chunkSize = (int)chunkSize;
		_taskCount = Math.Max(1, Math.Min(Environment.ProcessorCount, (int)(length / chunkSize)));
	}

	public async Task<Dictionary<Utf8StringRef, ItemData>> ProcessAsync()
	{
		var aggregates = new Dictionary<Utf8StringRef, ItemData>();
		var tasks = new Task[_taskCount];
		for (int i = 0; i < tasks.Length; i++)
		{
			tasks[i] = Task.Run(() => ProcessChunks(aggregates));
		}
		await Task.WhenAll(tasks);
		return aggregates;
	}

	private unsafe void ProcessChunks(Dictionary<Utf8StringRef, ItemData> aggregates)
	{
		var localAggregates = new Dictionary<Utf8StringRef, ItemData>();

		while (true)
		{
			nint start = GetNextChunk(out nint length);
			if (length == 0) break;
			Parser.Parse(localAggregates, ref Unsafe.AsRef<byte>((void*)start), ref Unsafe.AsRef<byte>((void*)(start + length)));
		}

		lock (aggregates)
		{
			foreach (var kvp in localAggregates)
			{
				ref var target = ref CollectionsMarshal.GetValueRefOrAddDefault(aggregates, kvp.Key, out bool exists);
				if (!exists)
				{
					target = kvp.Value;
				}
				else
				{
					target.Aggregate(kvp.Value);
				}
			}
		}
	}

	public unsafe nint GetNextChunk(out nint length)
	{
		nint chunkStart = Volatile.Read(ref _nextChunkStartPointer);

		while (true)
		{
			nint remainingLength = _endPointer - chunkStart;

			if (remainingLength == 0)
			{
				length = 0;
				return 0;
			}

			nint chunkSize;
			if (_chunkSize + ChunkExtraSizeTolerance >= remainingLength)
			{
				chunkSize = remainingLength;
			}
			else
			{
				// Start looking up for a new-line character from the theoretical last character of the chunk.
				// This supports the case when we would, by chance, perfectly hit the end of a line on a chunk boundary.
				int endOfLineIndex = new Span<byte>((void*)(chunkStart + _chunkSize - 1), ChunkExtraSizeTolerance + 1).IndexOf((byte)'\n');

				// This exception is put as a security, but we generally assume the data to be already correct in this program.
				if (endOfLineIndex < 0) throw new InvalidDataException("Could not find new line character in the searched window.");

				// The new line character is automatically skipped because the search for it was one character earlier.
				chunkSize = _chunkSize + endOfLineIndex;
			}

			nint oldChunkStart = Interlocked.CompareExchange(ref _nextChunkStartPointer, chunkStart + chunkSize, chunkStart);

			if (oldChunkStart == chunkStart)
			{
				length = chunkSize;
				return chunkStart;
			}

			chunkStart = oldChunkStart;
		}
	}
}
#endif

unsafe readonly struct Utf8StringRef(byte* pointer, int length) : IEquatable<Utf8StringRef>
{
	private readonly byte* _pointer = pointer;
	private readonly int _length = length;

	public ReadOnlySpan<byte> Value => new(_pointer, _length);

	public override string ToString() => Encoding.UTF8.GetString(Value);

	public override bool Equals([NotNullWhen(true)] object? obj)
		=> obj is Utf8StringRef str && Equals(str);

	public bool Equals(Utf8StringRef other) => Value.SequenceEqual(other.Value);

	public override int GetHashCode()
	{
		HashCode h = new();
		h.AddBytes(Value);
		return h.ToHashCode();
	}
}

[DebuggerDisplay("Min={Min}, Max={Max}, Sum={Sum} Count={Count}")]
struct ItemData
{
	public int Min;
	public int Max;
	public int Sum;
	public int Count;

	public ItemData(int value)
	{
		Min = value;
		Max = value;
		Sum = value;
		Count = 1;
	}

	public void Aggregate(int value)
	{
		Min = Math.Min(Min, value);
		Max = Math.Max(Max, value);
		Sum += value;
		Count++;
	}

	public void Aggregate(ItemData other)
	{
		Min = Math.Min(Min, other.Min);
		Max = Math.Max(Max, other.Max);
		Sum += other.Sum;
		Count += other.Count;
	}
}