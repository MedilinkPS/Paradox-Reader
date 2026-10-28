using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace ParadoxTest
{
    /// <summary>
    /// Command-line entry point for the ParadoxTest harness. Dispatches to:
    /// - <see cref="CorpusTest"/> for the schema-agnostic, data-folder-driven
    ///   regression run (the default / "corpustest" mode).
    /// - <see cref="LibUpdateTest"/> for the dedicated library update/create
    ///   tests, via <see cref="MiscTests"/> aliases.
    /// - <see cref="MiscTests"/> for all other one-off diagnostic/test modes
    ///   (SQLRunner-only mode, harness-only mode, step comparison, PX index
    ///   growth, password tests, and the legacy TESTTAB.DB "suite").
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
        {

            // New test I've hand made to verify writes are not having index out of date errors. I think it might be specifically multi field indices that are causing the problem.

            var databaseFolderPath = "c:\\temp\\data";
            var clientVersionLogWithoutExt = "ClientVersionLog";
            var clientVersionLogWithExt = $"{clientVersionLogWithoutExt}.db";
            var clientVersionLogWithWildcardExt = $"{clientVersionLogWithoutExt}.*";
            var clientVersionLogWildcardPath = Path.Combine(databaseFolderPath, clientVersionLogWithWildcardExt);
            var clientVersionLogPath = Path.Combine(databaseFolderPath, clientVersionLogWithExt);

            // TODO: make a backup copy of the clientVersionLogDatabase (i.e. use wildcard as there may be multiple files), we will restore it at the end of the test. This is important because we are going to be writing to it and we don't want to corrupt the original.

            try
            {


                var writeCount = 1;
                var clientVersionLogsToWrite = new List<Dictionary<string, object>>();

                for (int i = 0; i < writeCount; i++)
                {
                    // TODO: randomise these a bit. Make sure you leave Id as zero as it is an autoinc field and will be set by the database. Make sure you leave DateTime as DateTime.Now so that it is always different.
                    var logEntry = new Dictionary<string, object>();
                    logEntry.Add("Id", 0);
                    logEntry.Add("MajorVersion", 1);
                    logEntry.Add("MinorVersion", 2);
                    logEntry.Add("VersionName", "TestVersion");
                    logEntry.Add("VersionBuildDate", new DateTime(2024, 1, 15));
                    logEntry.Add("ComputerName", "TESTPC");
                    logEntry.Add("UserName", "testuser");
                    logEntry.Add("ServerBaseDir", "C:\\Test");
                    logEntry.Add("ClientId", 12345);
                    logEntry.Add("OperatingSystem", "Windows 11");
                    logEntry.Add("DateTime", DateTime.Now);
                    clientVersionLogsToWrite.Add(logEntry);
                }

                LegacyWriteRows.WriteRows(clientVersionLogsToWrite.Select(logEntry => (clientVersionLogWithExt, logEntry)).ToList());

                // TODO: now copy the legacy appended version of the clientVersionLogWildcardPath to temp and give it an approprate postfix i.e. legacy_yyyyMMdd_HHmmss.db so that we can compare it
                // TODO: restore the original clientVersionLogDatabase from the backup copy we made at the start of the test. This is important because we don't want to corrupt the original database.

                using (var table = new ParadoxReader.ParadoxTableFile(clientVersionLogPath))
                {
                    foreach(var columnValues in clientVersionLogsToWrite)
                    {
                        table.AppendRecord(columnValues.Values.ToArray());
                    }
                }

                // TODO: now copy the paradoxreader appended version of the clientVersionLogWildcardPath to temp and give it an approprate postfix i.e. paradoxreader_yyyyMMdd_HHmmss.db so that we can compare it
                // Restore the original clientVersionLogDatabase from the backup copy we made at the start of the test. This is important because we don't want to corrupt the original database.

                // TODO: write a binary comparison of the two databases, and if they are different, write a report of the differences to a text file in temp with an appropriate postfix i.e. differences_yyyyMMdd_HHmmss.txt so that we can investigate the differences.

            }
            finally
            {
                // Restore the original clientVersionLogDatabase from the backup copy we made at the start of the test. This is important because we don't want to corrupt the original database.
                // In theory we will have already done this, but we do it again anyway as a backup in case something went wrong and we didn't get to the restore step.
            }

            // End my new test.





            if (args.Length > 0 && args[0] == "libupdatetest")
            {
                MiscTests.RunLibUpdateTest();
                return;
            }

            if (args.Length > 0 && args[0] == "libcreatetest")
            {
                MiscTests.RunLibCreateTest();
                return;
            }

            if (args.Length > 0 && args[0] == "pxpasswordtest")
            {
                MiscTests.RunPxPasswordTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "pxpasswordwritetest")
            {
                string writeTestPath = args.Length > 1 ? args[1] : @"C:\temp\pxpwtest\testtab_passworded_withdata.DB";
                MiscTests.RunPxPasswordWriteTestMode(writeTestPath);
                return;
            }

            if (args.Length > 0 && args[0] == "rebuildtest")
            {
                MiscTests.RunRebuildTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "freshrebuildtest")
            {
                // Usage: ParadoxTest.exe freshrebuildtest [insertCount]
                // Creates a brand-new table via SQLRunner (real BDE), rebuilds
                // it with TableRebuilder, and compares rebuilt vs. pristine to
                // isolate whether TableRebuilder itself corrupts an
                // otherwise-known-good table, independent of any pre-existing
                // corruption in a corpus table. Defaults to 0 records (empty
                // table); pass 1 to test the smallest non-empty case.
                int insertCount = args.Length > 1 && int.TryParse(args[1], out var ic) ? ic : 0;
                MiscTests.RunFreshRebuildTest(insertCount);
                return;
            }

            if (args.Length > 0 && args[0] == "indexoutofdatetest")
            {
                MiscTests.RunIndexOutOfDateTestMode();
                return;
            }

            if (args.Length > 0 && args[0] == "sqlenginetest")
            {
                SqlEngineTest.Run();
                return;
            }

            if ((args.Length > 0 && args[0] == "corpustest") || args.Length == 0)
            {
                // Usage: ParadoxTest.exe [corpustest] [dataRoot] [maxTables] [filter]
                // Schema-agnostic test mode: walks every table found in
                // dataRoot (default: .\data relative to the current working
                // directory, i.e. bin\Debug\data when run from Visual
                // Studio/Test Explorer or the exe's own folder, falling back
                // to the CorpusDataRootPath appSetting if set), infers each
                // table's schema from its own
                // header, and exercises append/update/read/lookup operations
                // against it, comparing against SQLRunner where available.
                // By default only a small random sample of tables is
                // processed (maxTables=12) so a full corpus (which can be
                // hundreds of tables) isn't scanned unintentionally; pass an
                // explicit maxTables (e.g. 0 for no limit) to override.
                string dataRoot = args.Length > 1 ? args[1] : null;
                int maxTables = args.Length > 2 && int.TryParse(args[2], out var mt) ? mt : 12;
                string filter = args.Length > 3 ? args[3] : null;
#if NET10_0_OR_GREATER || NETFRAMEWORK
                Trace.Listeners.Add(new ConsoleTraceListener());
#endif
                CorpusTest.Run(dataRoot, maxTables, filter);
                return;
            }

            // Before doing anything else that might launch SQLRunner, make
            // sure we're starting from a genuinely clean state: no stray
            // SQLRunner process left over from a prior hung/killed run, and
            // no leftover *.LCK files. Without this, a previous crash can
            // silently leave a zombie SQLRunner (or lock) around that then
            // fights with the next run.
            MiscTests.EnsureCleanState();

            if (args.Length > 0 && args[0] == "sqlrunnermode")
            {
                MiscTests.RunSqlRunnerMode();
                return;
            }

            if (args.Length > 0 && args[0] == "harnessmode")
            {
                MiscTests.RunHarnessMode();
                return;
            }

            if (args.Length > 0 && args[0] == "comparesteps")
            {
                MiscTests.RunCompareStepsMode();
                return;
            }

            if (args.Length > 0 && args[0] == "comparerebuild")
            {
                // Usage: ParadoxTest.exe comparerebuild <dir> <baseNameA> <baseNameB>
                //     or ParadoxTest.exe comparerebuild <dirA> <baseNameA> <dirB> <baseNameB>
                // Byte-compares (and header-decodes) all files sharing a base
                // name (.DB/.MB/.PX/.XGn/.YGn) between two rebuilds, e.g. our
                // rebuild vs. the pdxrbld rebuild of the same source table:
                //   ParadoxTest.exe comparerebuild c:\temp\paradoxtest PatientBlobs_ourrebuild PatientBlobs_pdxrbldrebuild
                if (args.Length == 4)
                {
                    MiscTests.RunCompareRebuildMode(args[1], args[2], args[3]);
                }
                else if (args.Length == 5)
                {
                    MiscTests.RunCompareRebuildMode(args[1], args[2], args[3], args[4]);
                }
                else
                {
                    Console.WriteLine("Usage: ParadoxTest.exe comparerebuild <dir> <baseNameA> <baseNameB>");
                    Console.WriteLine("   or: ParadoxTest.exe comparerebuild <dirA> <baseNameA> <dirB> <baseNameB>");
                }
                return;
            }

            if (args.Length > 0 && args[0] == "growpxindex")
            {
                int targetCount = args.Length > 1 && int.TryParse(args[1], out var n) ? n : 5000;
                MiscTests.RunGrowPxIndexModePublic(targetCount);
                return;
            }

            if (args.Length > 0 && args[0] == "rebuildpath")
            {
                // Usage: ParadoxTest.exe rebuildpath <full path to .DB>
                // Runs TableRebuilder.Rebuild directly against an arbitrary
                // existing .DB file in-place (for ad hoc corpus verification).
                string dbPath = args[1];
                var result = ParadoxReader.TableRebuilder.Rebuild(dbPath);
                Console.WriteLine("Rebuilt {0}: {1} record(s) migrated.", dbPath, result.RecordsMigrated);
                return;
            }

            if (args.Length > 0 && args[0] == "checkindexoutofdate")
            {
                // Usage: ParadoxTest.exe checkindexoutofdate <full path to .DB>
                string dbPath = args[1];
                using (var t = new ParadoxReader.ParadoxTableFile(dbPath))
                {
                    Console.WriteLine("IndexOutOfDate = {0}", t.IndexOutOfDate);
                }
                return;
            }

            if (args.Length > 0 && args[0] == "headercomparetest")
            {
                // Usage: ParadoxTest.exe headercomparetest
                // Creates a range of schema shapes both via ParadoxReader
                // (TableCreator) and via SQLRunner (real BDE), byte-diffs
                // their headers at creation time and again after one insert,
                // to investigate whether our creation logic matches BDE's
                // "from scratch" byte layout.
                HeaderCompareTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "bdeconfigcomparetest")
            {
                // Usage: ParadoxTest.exe bdeconfigcomparetest
                // Swaps the live BDE idapi32.cfg between the
                // "original settings" (Paradox level 5, 2048-byte blocks) and
                // "larger block size" (Paradox level 7, 32768-byte blocks)
                // backups, creates every HeaderCompareTest schema shape via
                // SQLRunner (real BDE) under each, and byte-diffs the
                // resulting headers to isolate exactly what the BDE-level
                // version/block-size defaults change on disk.
                BdeConfigCompareTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "createtabletest")
            {
                // Usage: ParadoxTest.exe createtabletest
                // For each schema shape in HeaderCompareTest.BuildCases(),
                // creates the table via ParadoxReader's TableCreator and via
                // SQLRunner (real BDE CREATE TABLE/CREATE INDEX), does a full
                // byte comparison of every created file while both are still
                // blank, then inserts one equivalent record into BOTH sides
                // using ParadoxTableFile.InsertRecord (deliberately not
                // SQLRunner INSERT, to isolate create-time differences and
                // because SQLRunner may be unable to insert into a
                // corrupt/incompatible table our creation logic produced),
                // and repeats the full byte comparison.
                CreateTableCompareTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "createtablememotest")
            {
                // Usage: ParadoxTest.exe createtablememotest
                // Focused memo-value-set regression: drives both "ours"
                // (ParadoxReader) and "sqlrunner" (real BDE) through CREATE
                // TABLE -> INSERT (blank memo) -> UPDATE NOTES to 19/20/21-char
                // values, snapshotting and byte-comparing every stage to find
                // exactly where/when the two diverge (e.g. when the .MB file
                // is first touched at the inline/leader size boundary).
                CreateTableMemoTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "pkalpmemodiagtest")
            {
                // Usage: ParadoxTest.exe pkalpmemodiagtest
                // Recreates C:\TEMP\createtablecompare\PKALPMEMO\ours\PKALPMEM.DB
                // via our own TableCreator/AppendRecord/UpdateRecord, mirroring
                // the SQLRunner CREATE/INSERT/UPDATE flow used to (re)build the
                // sqlrunner-side reference fixture, so the two can be byte-diffed.
                PkAlpMemoDiagTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "rebuildcomparetest")
            {
                // Usage: ParadoxTest.exe rebuildcomparetest
                // For the same schema shapes as headercomparetest, builds
                // a pristine SQLRunner original (no data + 1 row), then
                // rebuilds copies of each via BDE's Pdxrbld.exe and via
                // ParadoxReader.TableRebuilder.Rebuild, producing six
                // datasets per case. Verifies each with a SQLRunner
                // "select count(*)" oracle and byte/header-diffs every
                // meaningful pairing (orig vs pdxrbld, orig vs ourrebuild,
                // pdxrbld vs ourrebuild) to isolate what our rebuild does
                // differently.
                RebuildCompareTest.Run();
                return;
            }

            if (args.Length > 0 && args[0] == "checkrebuildcount")
            {
                // Usage: ParadoxTest.exe checkrebuildcount <full path to .DB> [expectedCount]
                // Standalone SQLRunner "select count(*)" oracle check against
                // a single table, e.g. to confirm/reproduce a reported
                // rebuild failure before running the full comparison suite.
                // If expectedCount is omitted, just prints the parsed count
                // without a PASS/FAIL verdict (SQLRunner reports "Read 1
                // rows." even for empty tables, so 0 is never a meaningful
                // expectation here).
                string countDbPath = args[1];
                int expectedCount = args.Length > 2 ? int.Parse(args[2]) : -1;
                RebuildCompareTest.CheckCount(countDbPath, expectedCount);
                return;
            }

            if (args.Length > 0 && args[0] == "suite")
            {
                // Usage: ParadoxTest.exe suite [TABLENAME.DB]
                // Runs the standard Test 1-6 suite against the given fixture
                // table (must exist in ParadoxTest\data alongside its
                // .PX/.MB/secondary index files). Defaults to TESTTAB.DB.
                string tableName = args.Length > 1 ? args[1] : null;
                MiscTests.RunSuiteMode(tableName);
                return;
            }
        }
    }
}
