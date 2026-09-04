using System.Collections.Generic;

namespace ParadoxReader
{
    public class ParadoxPrimaryKey : ParadoxFile
    {
        private readonly ParadoxFile table;

        public ParadoxPrimaryKey(ParadoxFile table, string filePath)
            : base(filePath)
        {
            this.table = table;
        }

        public IEnumerable<ParadoxReader.ParadoxRecord> Enumerate(ParadoxCondition condition)
        {
            return Enumerate(condition, (ushort)(this.pxRootBlockId-1), this.pxLevelCount);
        }

        private IEnumerable<ParadoxReader.ParadoxRecord> Enumerate(ParadoxCondition condition, ushort blockNumber, int indexLevel)
        {
            if (indexLevel == 0)
            {
                var block = this.table.GetBlock(blockNumber);
                for (int i=0; i<block.RecordCount; i++)
                {
                    var rec = block[i];
                    if (condition.IsDataOk(rec))
                    {
                        yield return rec;
                    }
                }
            }
            else
            {
                var block = this.GetBlock(blockNumber);
                var blockIdFldIndex = this.FieldCount - 3;
                for (int i = 0; i < block.RecordCount; i++)
                {
                    var rec = block[i];
                    if (condition.IsIndexPossible(rec, i < block.RecordCount-1 ? block[i + 1] : null))
                    {
                        var qry = Enumerate(condition, (ushort)((short) rec.DataValues[blockIdFldIndex]-1), indexLevel - 1);
                        foreach (var dataRec in qry)
                        {
                            yield return dataRec;
                        }
                    }
                }
            }
        }
    }
}
