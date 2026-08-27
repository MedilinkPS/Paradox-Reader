namespace ParadoxReader
{
    /// <summary>
    /// Represents a single entry in a Paradox B-tree node (.PX or .Xnn file).
    /// Each entry contains the primary key field values and a pointer to either
    /// a child block (internal node) or a data record (leaf node).
    /// </summary>
    internal class PxEntry
    {
        /// <summary>Raw serialized bytes of the key fields.</summary>
        public byte[] KeyData { get; set; }

        /// <summary>
        /// For internal nodes: block number of the right child in the index file.
        /// For leaf nodes:     block number in the .DB file where the record lives.
        /// </summary>
        public ushort BlockNumber { get; set; }

        /// <summary>
        /// Record index within the block (0-based).
        /// Only meaningful for leaf nodes.
        /// </summary>
        public ushort RecordOffset { get; set; }

        public PxEntry(byte[] keyData, ushort blockNumber, ushort recordOffset)
        {
            KeyData      = keyData;
            BlockNumber  = blockNumber;
            RecordOffset = recordOffset;
        }

        /// <summary>
        /// Total size of this entry on disk:
        /// key data bytes + 2 bytes block number + 2 bytes record offset.
        /// </summary>
        public int DiskSize => KeyData.Length + 4;
    }
}