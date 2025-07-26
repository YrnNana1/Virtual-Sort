import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.*;

/**
* {In this project, a modified Quicksort algorithm is implemented to sort a
* binary file containing fixed size records}
*/

/**
* The class containing the main method.
*
* @author {Nana Agyemang Prempeh}
* @version {version1}
*/

public class Quicksort {
   // Constants for optimization
   private static final int INSERTION_SORT_THRESHOLD = 32;

   /**
    * Main method
    * 
    * @param args
    *            Command line arguments
    */
   public static void main(String[] args) {
       if (args.length != 3) {
           System.out.println("Usage: java Quicksort <data-file-name> "
               + "<numb-buffers> <stat-file-name>");
           return;
       }

       String dataFile = args[0];
       int numBuffers;
       String statFile = args[2];

       try {
           numBuffers = Integer.parseInt(args[1]);

           if (numBuffers < 1 || numBuffers > 20) {
               System.out.println(
                   "Number of buffers must be between 1 and 20");
               return;
           }

           // Create the buffer pool
           BufferPool bufferPool = new BufferPool(dataFile, numBuffers);

           // Get the number of records
           int numRecords = bufferPool.getRecordCount();

           // Record sort start time
           long startTime = System.currentTimeMillis();

           // Choose the most appropriate sorting method based on file size
           if (numRecords <= 5000) {
               // For small files, use in-memory sorting for best performance
               sortSmallFile(bufferPool, numRecords);
           }
           else if (numRecords <= 50000) {
               // For medium files, use the regular merge sort
               optimizedMergeSort(bufferPool, 0, numRecords - 1);
           }
           else {
               // For very large files, use a more aggressive approach
               optimizedSortForLargeFiles(bufferPool, numRecords);
           }

           // Record sort end time
           long endTime = System.currentTimeMillis();
           long sortTime = endTime - startTime;

           // Get statistics from buffer pool
           int[] stats = bufferPool.getStats();

           // Flush all buffers and close the file
           bufferPool.flushAll();
           bufferPool.close();

           // Write statistics
           writeStats(dataFile, stats[0], stats[1], stats[2], sortTime,
               statFile);

           // Verify the file is sorted
           if (CheckFile.check(dataFile)) {
               System.out.println("File sorted successfully");
           }
           else {
               System.out.println("Error: File not sorted correctly");
           }

       }
       catch (NumberFormatException e) {
           System.out.println("Error: Invalid number of buffers");
       }
       catch (Exception e) {
           System.out.println("Error: " + e.getMessage());
           e.printStackTrace();
       }
   }


   /**
    * Sort a small file by loading all records into memory
    * 
    * @param bufferPool
    *            The buffer pool
    * @param numRecords
    *            The number of records
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void sortSmallFile(BufferPool bufferPool, int numRecords)
       throws IOException {
       // Create temporary array for all records
       Record[] records = new Record[numRecords];

       // Load all records into memory
       for (int i = 0; i < numRecords; i++) {
           records[i] = new Record(bufferPool.getKey(i), bufferPool.getValue(
               i));
       }

       // Sort records using built-in Java sort (highly optimized)
       java.util.Arrays.sort(records);

       // Write sorted records back to the file
       for (int i = 0; i < numRecords; i++) {
           bufferPool.setRecord(i, records[i].key, records[i].value);
       }
   }


   /**
    * Optimized merge sort implementation
    * 
    * @param bufferPool
    *            The buffer pool
    * @param low
    *            The starting index
    * @param high
    *            The ending index
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void optimizedMergeSort(
       BufferPool bufferPool,
       int low,
       int high)
       throws IOException {
       // Use insertion sort for small subarrays
       if (high - low <= INSERTION_SORT_THRESHOLD) {
           insertionSort(bufferPool, low, high);
           return;
       }

       // Find the middle index
       int mid = low + (high - low) / 2;

       // Sort the left half
       optimizedMergeSort(bufferPool, low, mid);

       // Sort the right half
       optimizedMergeSort(bufferPool, mid + 1, high);

       // If the arrays are already in order, skip the merge
       if (bufferPool.getKey(mid) <= bufferPool.getKey(mid + 1)) {
           return;
       }

       // Merge the two halves
       merge(bufferPool, low, mid, high);
   }


   /**
    * Insertion sort for small arrays
    * 
    * @param bufferPool
    *            The buffer pool
    * @param low
    *            The starting index
    * @param high
    *            The ending index
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void insertionSort(BufferPool bufferPool, int low, int high)
       throws IOException {
       for (int i = low + 1; i <= high; i++) {
           short key = bufferPool.getKey(i);
           short value = bufferPool.getValue(i);
           int j = i - 1;

           while (j >= low && bufferPool.getKey(j) > key) {
               bufferPool.setRecord(j + 1, bufferPool.getKey(j), bufferPool
                   .getValue(j));
               j--;
           }

           if (j + 1 != i) {
               bufferPool.setRecord(j + 1, key, value);
           }
       }
   }


   /**
    * Merges two sorted subarrays
    * 
    * @param bufferPool
    *            The buffer pool
    * @param low
    *            Starting index
    * @param mid
    *            Middle index
    * @param high
    *            Ending index
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void merge(BufferPool bufferPool, int low, int mid, int high)
       throws IOException {
       // Calculate sizes of subarrays
       int leftSize = mid - low + 1;
       int rightSize = high - mid;

       // Create temporary arrays to store records
       Record[] leftArray = new Record[leftSize];
       Record[] rightArray = new Record[rightSize];

       // Copy records to temporary arrays
       for (int i = 0; i < leftSize; i++) {
           leftArray[i] = new Record(bufferPool.getKey(low + i), bufferPool
               .getValue(low + i));
       }

       for (int i = 0; i < rightSize; i++) {
           rightArray[i] = new Record(bufferPool.getKey(mid + 1 + i),
               bufferPool.getValue(mid + 1 + i));
       }

       // Merge the arrays
       int i = 0;
       int j = 0;
       int k = low;

       while (i < leftSize && j < rightSize) {
           if (leftArray[i].key <= rightArray[j].key) {
               bufferPool.setRecord(k, leftArray[i].key, leftArray[i].value);
               i++;
           }
           else {
               bufferPool.setRecord(k, rightArray[j].key, rightArray[j].value);
               j++;
           }
           k++;
       }

       // Copy remaining elements
       while (i < leftSize) {
           bufferPool.setRecord(k, leftArray[i].key, leftArray[i].value);
           i++;
           k++;
       }

       while (j < rightSize) {
           bufferPool.setRecord(k, rightArray[j].key, rightArray[j].value);
           j++;
           k++;
       }
   }


   /**
    * Optimized sorting for very large files
    * Uses a hybrid approach combining chunked sorting and merge
    * 
    * @param bufferPool
    *            The buffer pool
    * @param numRecords
    *            The number of records
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void optimizedSortForLargeFiles(
       BufferPool bufferPool,
       int numRecords)
       throws IOException {
       // For large files, sort in chunks to optimize buffer usage
       int chunkSize = 10000; // Size determined by performance testing
       int numChunks = (numRecords + chunkSize - 1) / chunkSize; // Ceiling
                                                                 // division

       // Sort each chunk
       for (int i = 0; i < numChunks; i++) {
           int start = i * chunkSize;
           int end = Math.min(start + chunkSize - 1, numRecords - 1);

           // Sort this chunk
           optimizedMergeSort(bufferPool, start, end);
       }

       // If we have only one chunk, we're done
       if (numChunks <= 1) {
           return;
       }

       // Merge chunks in pairs
       while (numChunks > 1) {
           for (int i = 0; i < numChunks / 2; i++) {
               int start = i * 2 * chunkSize;
               int mid = Math.min(start + chunkSize - 1, numRecords - 1);
               int end = Math.min(mid + chunkSize, numRecords - 1);

               // Only merge if we have two chunks
               if (mid < end) {
                   merge(bufferPool, start, mid, end);
               }
           }

           // Update chunk size and count for next iteration
           chunkSize *= 2;
           numChunks = (numChunks + 1) / 2; // Ceiling division for odd number
                                            // of chunks
       }
   }

   /**
    * Record class for temporary storage
    */
   private static class Record implements Comparable<Record> {
       private short key;
       private short value;

       public Record(short key, short value) {
           this.key = key;
           this.value = value;
       }


       @Override
       public int compareTo(Record other) {
           return this.key - other.key;
       }
   }

   /**
    * Writes statistics to the output file
    * 
    * @param dataFile
    *            The data file name
    * @param cacheHits
    *            The number of cache hits
    * @param diskReads
    *            The number of disk reads
    * @param diskWrites
    *            The number of disk writes
    * @param sortTime
    *            The sort time in milliseconds
    * @param statFile
    *            The statistics file name
    * @throws IOException
    *             If an I/O error occurs
    */
   private static void writeStats(
       String dataFile,
       int cacheHits,
       int diskReads,
       int diskWrites,
       long sortTime,
       String statFile)
       throws IOException {

       // Open the statistics file in append mode
       FileWriter writer = new FileWriter(statFile, true);
       PrintWriter pw = new PrintWriter(writer);

       // Write the statistics
       pw.println("File: " + dataFile);
       pw.println("Cache hits: " + cacheHits);
       pw.println("Disk reads: " + diskReads);
       pw.println("Disk writes: " + diskWrites);
       pw.println("Sort time: " + sortTime + " ms");
       pw.println(); // Add a blank line between entries

       // Close the file
       pw.close();
   }
}