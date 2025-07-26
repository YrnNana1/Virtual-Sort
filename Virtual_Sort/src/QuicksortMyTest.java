import java.io.File;
import java.io.IOException;
import student.TestCase;

/**
* Extended test class for the Quicksort implementation
* 
* @author Nana Agyemang Prempeh
* @version version1
*/
public class QuicksortMyTest extends TestCase {

   /**
    * Sets up the tests that follow
    */
   public void setUp() throws Exception {
       super.setUp();
       systemOut().clearHistory();
   }


   /**
    * Tests sorting a small file with a single buffer
    * This verifies the basic functionality of the quicksort algorithm
    */
   public void testSortSmallFile() throws Exception {
       // Generating a small file with 1 block
       String dataFile = "smallTest.bin";
       FileGenerator generator = new FileGenerator(dataFile, 1);
       generator.setSeed(12345); // Using a seed for reproducible results
       generator.generateFile(FileType.BINARY);

       // Verifying the file is not sorted initially
       assertFalse(CheckFile.check(dataFile));

       // Sorting the file
       String[] args = { dataFile, "1", "smallStats.txt" };
       Quicksort.main(args);

       // Verifying the file is sorted
       assertTrue(CheckFile.check(dataFile));

       // Cleaning up
       File file = new File(dataFile);
       file.delete();
       File statsFile = new File("smallStats.txt");
       if (statsFile.exists()) {
           statsFile.delete();
       }
   }


   /**
    * Tests sorting a medium-sized file with multiple buffers
    * This tests the efficiency of buffer usage
    */
   public void testSortMediumFile() throws Exception {
       // Generating a medium file with 10 blocks
       String dataFile = "mediumTest.bin";
       FileGenerator generator = new FileGenerator(dataFile, 10);
       generator.setSeed(54321); // Use a seed for reproducible results
       generator.generateFile(FileType.BINARY);

       // Verifying the file is not sorted initially
       assertFalse(CheckFile.check(dataFile));

       // Sorting the file
       String[] args = { dataFile, "5", "mediumStats.txt" };
       Quicksort.main(args);

       // Verifying the file is sorted
       assertTrue(CheckFile.check(dataFile));

       // Cleaning up
       File file = new File(dataFile);
       file.delete();
       File statsFile = new File("mediumStats.txt");
       if (statsFile.exists()) {
           statsFile.delete();
       }
   }


   /**
    * Tests sorting a larger file with multiple buffers
    * This tests the scalability of the algorithm
    */
   public void testSortLargeFile() throws Exception {
       // Generating a larger file with 50 blocks
       String dataFile = "largeTest.bin";
       FileGenerator generator = new FileGenerator(dataFile, 50);
       generator.setSeed(67890); // Use a seed for reproducible results
       generator.generateFile(FileType.BINARY);

       // Verifying the file is not sorted initially
       assertFalse(CheckFile.check(dataFile));

       // Sorting the file
       String[] args = { dataFile, "10", "largeStats.txt" };
       Quicksort.main(args);

       // Verifying the file is sorted
       assertTrue(CheckFile.check(dataFile));

       // Cleaning up
       File file = new File(dataFile);
       file.delete();
       File statsFile = new File("largeStats.txt");
       if (statsFile.exists()) {
           statsFile.delete();
       }
   }


   /**
    * Tests sorting with varying buffer counts to evaluate performance
    * This helps identify the optimal buffer count
    */
   public void testBufferCountPerformance() throws Exception {
       // Generating a medium file with 20 blocks
       String dataFile = "perfTest.bin";
       FileGenerator generator = new FileGenerator(dataFile, 20);
       generator.setSeed(98765); // Use a seed for reproducible results
       generator.generateFile(FileType.BINARY);

       // Tests with different buffer counts
       int[] bufferCounts = { 1, 5, 10 };
       String statFile = "perfStats.txt";
       long[] sortTimes = new long[bufferCounts.length];

       for (int i = 0; i < bufferCounts.length; i++) {
           int buffers = bufferCounts[i];

           // Creating a copy of the original file for each test
           String testFile = "perfTest" + buffers + ".bin";
           copyFile(dataFile, testFile);

           // Record start time
           long startTime = System.currentTimeMillis();

           // Sorting the file
           String[] args = { testFile, String.valueOf(buffers), statFile };
           Quicksort.main(args);

           // Record end time
           long endTime = System.currentTimeMillis();
           sortTimes[i] = endTime - startTime;

           // Verifying the file is sorted
           assertTrue(CheckFile.check(testFile));

           // Cleaning up
           File file = new File(testFile);
           file.delete();
       }

       // just checking that the implementation handles different
       // buffer counts
       for (int i = 0; i < sortTimes.length; i++) {
           System.out.println("Buffer count " + bufferCounts[i] + ": "
               + sortTimes[i] + "ms");
       }

       // Cleaning up original file
       File file = new File(dataFile);
       file.delete();
       File statsFile = new File(statFile);
       if (statsFile.exists()) {
           statsFile.delete();
       }
   }


   /**
    * Tests handling of edge cases including minimum buffer count
    * and ASCII file sorting
    */
   public void testEdgeCases() throws Exception {
       // Test with minimum buffer count (1)
       String dataFile = "edgeTest.bin";
       FileGenerator generator = new FileGenerator(dataFile, 5);
       generator.setSeed(24680);
       generator.generateFile(FileType.BINARY);

       String[] args = { dataFile, "1", "edgeStats.txt" };
       Quicksort.main(args);

       assertTrue(CheckFile.check(dataFile));

       // Clean up
       File file = new File(dataFile);
       file.delete();

       // Test with ASCII file
       String asciiFile = "asciiTest.txt";
       FileGenerator asciiGenerator = new FileGenerator(asciiFile, 5);
       asciiGenerator.setSeed(13579);
       asciiGenerator.generateFile(FileType.ASCII);

       String[] asciiArgs = { asciiFile, "3", "asciiStats.txt" };
       Quicksort.main(asciiArgs);

       assertTrue(CheckFile.check(asciiFile));

       // Clean up
       File asciiFileObj = new File(asciiFile);
       asciiFileObj.delete();

       File statsFile = new File("edgeStats.txt");
       if (statsFile.exists()) {
           statsFile.delete();
       }

       File asciiStatsFile = new File("asciiStats.txt");
       if (asciiStatsFile.exists()) {
           asciiStatsFile.delete();
       }
   }


   /**
    * Tests invalid command line arguments
    */
   public void testInvalidArguments() {
       // Test with too few arguments
       String[] args1 = { "file.bin" };
       Quicksort.main(args1);
       String output1 = systemOut().getHistory();
       assertTrue(output1.contains("Usage: java Quicksort"));

       // Test with invalid buffer count
       String[] args2 = { "file.bin", "thirty", "stats.txt" };
       Quicksort.main(args2);
       String output2 = systemOut().getHistory();
       assertTrue(output2.contains("Error: Invalid number of buffers"));

       // Test with buffer count out of range
       String[] args3 = { "file.bin", "30", "stats.txt" };
       Quicksort.main(args3);
       String output3 = systemOut().getHistory();
       assertTrue(output3.contains("Number of buffers must be "
           + "between 1 and 20"));
   }


   /**
    * Helper method to copy a file
    */
   private void copyFile(String source, String destination)
       throws IOException {
       java.nio.file.Files.copy(java.nio.file.Paths.get(source),
           java.nio.file.Paths.get(destination),
           java.nio.file.StandardCopyOption.REPLACE_EXISTING);
   }

}