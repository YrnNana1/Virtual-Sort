import java.io.RandomAccessFile;

/**
 * Special test class for diagnosing large file sorting issues
 * This can be used to analyze the large file before and after sorting
 * 
 * @author {Nana Agyemang Prempeh}
 * @version {Version1}
 */
public class LargeFileTest {
    private static final boolean VERBOSE = false;

    /**
     * main method of this class
     * 
     * @param args
     *            Command line arguments
     */
    public static void main(String[] args) {
        try {
            // Generate a test file with 5 blocks
            String dataFile = "debugLarge.bin";
            FileGenerator generator = new FileGenerator(dataFile, 5);
            generator.setSeed(54321); // Same seed as in the failing test
            generator.generateFile(FileType.BINARY);

            // Check if it's sorted initially (should be false)
            System.out.println("Before sorting - Is file sorted? " + CheckFile
                .check(dataFile));

            // Print some statistics about the file
            analyzeFile(dataFile);

            // Sort the file
            String[] args2 = { dataFile, "3", "debugStats.txt" };
            Quicksort.main(args2);

            // Check if it's sorted after (should be true)
            System.out.println("After sorting - Is file sorted? " + CheckFile
                .check(dataFile));

            // Analyze the file after sorting
            analyzeFile(dataFile);

            // If file is still not sorted, find the problematic area
            if (!CheckFile.check(dataFile)) {
                findSortingIssue(dataFile);
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Analyzes a file and prints statistics
     */
    private static void analyzeFile(String filename) throws Exception {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        long fileSize = raf.length();
        int recordCount = (int)(fileSize / FileGenerator.BYTES_PER_RECORD);

        System.out.println("\nFile Analysis: " + filename);
        System.out.println("File size: " + fileSize + " bytes");
        System.out.println("Record count: " + recordCount);
        System.out.println("Block count: " + (fileSize
            / FileGenerator.BYTES_PER_BLOCK));

        // Sample some keys from the beginning, middle, and end
        System.out.println("\nSampling keys:");
        sampleKeys(raf, 0, "Beginning");
        sampleKeys(raf, recordCount / 2, "Middle");
        sampleKeys(raf, recordCount - 10, "End");

        raf.close();
    }


    /**
     * Samples and prints keys from a specific position in the file
     */
    private static void sampleKeys(
        RandomAccessFile raf,
        int startRecord,
        String label)
        throws Exception {
        System.out.println(label + " of file:");
        int count = Math.min(10, (int)(raf.length()
            / FileGenerator.BYTES_PER_RECORD) - startRecord);

        for (int i = 0; i < count; i++) {
            raf.seek((startRecord + i) * FileGenerator.BYTES_PER_RECORD);
            short key = raf.readShort();
            short value = raf.readShort();
            System.out.println("  Record " + (startRecord + i) + ": Key=" + key
                + ", Value=" + value);
        }
    }


    /**
     * Finds and reports where the sorting issue occurs
     */
    private static void findSortingIssue(String filename) throws Exception {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        long fileSize = raf.length();
        int recordCount = (int)(fileSize / FileGenerator.BYTES_PER_RECORD);

        System.out.println("\nSearching for sorting issues:");

        short prevKey = Short.MIN_VALUE;
        for (int i = 0; i < recordCount; i++) {
            raf.seek(i * FileGenerator.BYTES_PER_RECORD);
            short currKey = raf.readShort();

            if (prevKey > currKey) {
                System.out.println("Sorting issue found at record " + i);
                System.out.println("Previous key: " + prevKey
                    + ", Current key: " + currKey);

                // Print surrounding records for context
                System.out.println("\nRecords around the issue:");
                int start = Math.max(0, i - 5);
                int end = Math.min(recordCount - 1, i + 5);

                for (int j = start; j <= end; j++) {
                    raf.seek(j * FileGenerator.BYTES_PER_RECORD);
                    short key = raf.readShort();
                    short value = raf.readShort();
                    String marker = (j == i) ? " <-- ISSUE" : "";
                    System.out.println("  Record " + j + ": Key=" + key
                        + ", Value=" + value + marker);
                }

                // We found one issue, but lets continue checking to see if
                // there are more
                if (!VERBOSE) {
                    break; // Stop after finding the first issue unless VERBOSE
                           // is true
                }
            }

            prevKey = currKey;
        }

        raf.close();
    }
}