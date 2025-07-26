import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
* BufferPool class, manages a pool of buffers using LRU replacement policy.
* All accesses to the data file are mediated through this class.
* 
* @author Nana Agyemang Prempeh
* @version version1
*/
public class BufferPool {
   // Constants for debugging
   private static final boolean DEBUG = false;

   private byte[][] buffers; // Array of buffer blocks
   private int[] blockIds; // Block ID (file block number) for each buffer
   private long[] timestamps; // Timestamp for LRU tracking
   private boolean[] dirtyFlags; // Dirty flags to mark modified buffers
   private int numBuffers; // Number of buffers in the pool
   private String filename; // Name of the data file
   private RandomAccessFile file; // File access

   // Statistics
   private int cacheHits;
   private int diskReads;
   private int diskWrites;

   /**
    * Constructor - Creates a buffer pool for the specified file
    * 
    * @param filename
    *            The name of the data file
    * @param numBuffers
    *            The number of buffers in the pool
    */
   public BufferPool(String filename, int numBuffers) throws IOException {
       this.filename = filename;
       this.numBuffers = numBuffers;

       // Initialize buffer pool
       buffers = new byte[numBuffers][FileGenerator.BYTES_PER_BLOCK];
       blockIds = new int[numBuffers];
       timestamps = new long[numBuffers];
       dirtyFlags = new boolean[numBuffers];

       // Initialize all block IDs to -1 (invalid)
       for (int i = 0; i < numBuffers; i++) {
           blockIds[i] = -1;
       }

       // Open the file
       file = new RandomAccessFile(filename, "rw");

       // Initialize statistics
       cacheHits = 0;
       diskReads = 0;
       diskWrites = 0;
   }


   /**
    * Gets the buffer for the specified block
    * 
    * @param blockId
    *            The block ID to get
    * @return The buffer for the block
    */
   public byte[] getBlock(int blockId) throws IOException {
       // Check if the block is already in the buffer pool
       int bufferIndex = findBufferForBlock(blockId);

       if (bufferIndex != -1) {
           // Cache hit - Block is already in buffer
           cacheHits++;
           updateTimestamp(bufferIndex);
           return buffers[bufferIndex];
       }

       // Cache miss - Need to load the block
       bufferIndex = getLRUBufferIndex();

       // If the LRU buffer is dirty, write it back to disk
       if (dirtyFlags[bufferIndex]) {
           writeBuffer(bufferIndex);
       }

       // Load the new block into the buffer
       loadBlock(blockId, bufferIndex);

       return buffers[bufferIndex];
   }


   /**
    * Marks a buffer as dirty (modified)
    * 
    * @param blockId
    *            The block ID to mark as dirty
    */
   public void markDirty(int blockId) throws IOException {
       int bufferIndex = findBufferForBlock(blockId);
       if (bufferIndex != -1) {
           dirtyFlags[bufferIndex] = true;
       }
       else {
           // If the block isn't in memory, load it first then mark it dirty
           getBlock(blockId);
           bufferIndex = findBufferForBlock(blockId);
           if (bufferIndex != -1) {
               dirtyFlags[bufferIndex] = true;
           }
           else {
               throw new IOException("Failed to load block for marking dirty: "
                   + blockId);
           }
       }
   }


   /**
    * Flushes all dirty buffers to disk
    */
   public void flushAll() throws IOException {
       if (DEBUG) {
           System.out.println("Flushing all dirty buffers to disk");
       }

       int flushCount = 0;
       for (int i = 0; i < numBuffers; i++) {
           if (blockIds[i] != -1 && dirtyFlags[i]) {
               writeBuffer(i);
               flushCount++;
           }
       }

       if (DEBUG) {
           System.out.println("Flushed " + flushCount + " dirty buffers");
       }
   }


   /**
    * Closes the buffer pool and file
    */
   public void close() throws IOException {
       flushAll();
       file.close();
   }


   /**
    * Finds the buffer containing the specified block
    * 
    * @param blockId
    *            The block ID to find
    * @return The buffer index or -1 if not found
    */
   private int findBufferForBlock(int blockId) {
       for (int i = 0; i < numBuffers; i++) {
           if (blockIds[i] == blockId) {
               return i;
           }
       }
       return -1;
   }


   /**
    * Gets the index of the least recently used buffer
    * 
    * @return The index of the LRU buffer
    */
   private int getLRUBufferIndex() {
       // First, look for an empty buffer (blockId == -1)
       for (int i = 0; i < numBuffers; i++) {
           if (blockIds[i] == -1) {
               return i;
           }
       }

       // No empty buffers, find the least recently used
       int lruIndex = 0;
       long oldestTime = timestamps[0];

       for (int i = 1; i < numBuffers; i++) {
           if (timestamps[i] < oldestTime) {
               oldestTime = timestamps[i];
               lruIndex = i;
           }
       }

       if (DEBUG) {
           System.out.println("LRU buffer: " + lruIndex + ", blockId: "
               + blockIds[lruIndex] + ", dirty: " + dirtyFlags[lruIndex]);
       }

       return lruIndex;
   }


   /**
    * Updates the timestamp for a buffer
    * 
    * @param bufferIndex
    *            The buffer index to update
    */
   private void updateTimestamp(int bufferIndex) {
       timestamps[bufferIndex] = System.currentTimeMillis();
   }


   /**
    * Writes a buffer back to disk
    * 
    * @param bufferIndex
    *            The buffer index to write
    */
   private void writeBuffer(int bufferIndex) throws IOException {
       if (blockIds[bufferIndex] != -1) {
           long position = (long)blockIds[bufferIndex]
               * FileGenerator.BYTES_PER_BLOCK;
           file.seek(position);
           file.write(buffers[bufferIndex]);
           diskWrites++;
           dirtyFlags[bufferIndex] = false;
       }
   }


   /**
    * Loads a block from disk into a buffer
    * 
    * @param blockId
    *            The block ID to load
    * @param bufferIndex
    *            The buffer index to use
    */
   private void loadBlock(int blockId, int bufferIndex) throws IOException {
       long position = (long)blockId * FileGenerator.BYTES_PER_BLOCK;

       // Make sure we don't read past the end of the file
       long fileLength = file.length();
       if (position >= fileLength) {
           throw new IOException(
               "Attempted to read past end of file, blockId: " + blockId);
       }

       // Seek to the position in the file
       file.seek(position);

       // Read the block
       int bytesRead = file.read(buffers[bufferIndex]);

       // Check if we read the full block
       if (bytesRead != FileGenerator.BYTES_PER_BLOCK) {
           // If at end of file, pad with zeros
           if (position + bytesRead == fileLength) {
               // Fill the rest with zeros
               for (int i =
                   bytesRead; i < FileGenerator.BYTES_PER_BLOCK; i++) {
                   buffers[bufferIndex][i] = 0;
               }
           }
           else {
               throw new IOException("Incomplete block read, expected "
                   + FileGenerator.BYTES_PER_BLOCK + " bytes, got "
                   + bytesRead);
           }
       }

       // Update buffer metadata
       blockIds[bufferIndex] = blockId;
       updateTimestamp(bufferIndex);
       dirtyFlags[bufferIndex] = false;
       diskReads++;
   }


   /**
    * Gets statistics about the buffer pool usage
    * 
    * @return An array with [cacheHits, diskReads, diskWrites]
    */
   public int[] getStats() {
       return new int[] { cacheHits, diskReads, diskWrites };
   }


   /**
    * Gets the total number of records in the file
    * 
    * @return The number of records
    */
   public int getRecordCount() throws IOException {
       // Get the file length
       long fileLength = file.length();

       // Calculate the number of records
       // Each record is BYTES_PER_RECORD bytes
       return (int)(fileLength / FileGenerator.BYTES_PER_RECORD);
   }


   /**
    * Gets a record's key value
    * 
    * @param recordIndex
    *            The index of the record
    * @return The key value
    */
   public short getKey(int recordIndex) throws IOException {
       if (recordIndex < 0) {
           throw new IOException("Invalid record index: " + recordIndex);
       }

       // Calculate block ID and offset within block
       int blockId = recordIndex / FileGenerator.RECORDS_PER_BLOCK;
       int blockOffset = (recordIndex % FileGenerator.RECORDS_PER_BLOCK)
           * FileGenerator.BYTES_PER_RECORD;

       // Get the buffer containing the block
       byte[] buffer = getBlock(blockId);

       // Create a ByteBuffer to read the key
       ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
       return byteBuffer.getShort(blockOffset);
   }


   /**
    * Gets a record's value
    * 
    * @param recordIndex
    *            The index of the record
    * @return The value
    */
   public short getValue(int recordIndex) throws IOException {
       // Calculate block ID and offset within block
       int blockId = recordIndex / FileGenerator.RECORDS_PER_BLOCK;
       int blockOffset = (recordIndex % FileGenerator.RECORDS_PER_BLOCK)
           * FileGenerator.BYTES_PER_RECORD;

       // Get the buffer containing the block
       byte[] buffer = getBlock(blockId);

       // Create a ByteBuffer to read the value (after the key)
       ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
       return byteBuffer.getShort(blockOffset + FileGenerator.BYTES_IN_KEY);
   }


   /**
    * Sets a record in the file
    * 
    * @param recordIndex
    *            The index of the record
    * @param key
    *            The key value
    * @param value
    *            The data value
    */
   public void setRecord(int recordIndex, short key, short value)
       throws IOException {
       if (recordIndex < 0) {
           throw new IOException("Invalid record index: " + recordIndex);
       }

       // Calculate block ID and offset within block
       int blockId = recordIndex / FileGenerator.RECORDS_PER_BLOCK;
       int blockOffset = (recordIndex % FileGenerator.RECORDS_PER_BLOCK)
           * FileGenerator.BYTES_PER_RECORD;

       // Get the buffer containing the block
       byte[] buffer = getBlock(blockId);

       // Create a ByteBuffer to write the record
       ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
       byteBuffer.putShort(blockOffset, key);
       byteBuffer.putShort(blockOffset + FileGenerator.BYTES_IN_KEY, value);

       // Mark the buffer as dirty
       markDirty(blockId);
   }


   /**
    * Swaps two records in the file
    * 
    * @param i
    *            The index of the first record
    * @param j
    *            The index of the second record
    */
   public void swapRecords(int i, int j) throws IOException {
       // If records are the same, do nothing
       if (i == j) {
           return;
       }

       if (i < 0 || j < 0) {
           throw new IOException("Invalid record indices for swap: " + i + ", "
               + j);
       }

       // Calculate block IDs and offsets within blocks
       int blockIdI = i / FileGenerator.RECORDS_PER_BLOCK;
       int blockIdJ = j / FileGenerator.RECORDS_PER_BLOCK;

       int offsetI = (i % FileGenerator.RECORDS_PER_BLOCK)
           * FileGenerator.BYTES_PER_RECORD;
       int offsetJ = (j % FileGenerator.RECORDS_PER_BLOCK)
           * FileGenerator.BYTES_PER_RECORD;

       // If the records are in the same block, we can optimize by doing less
       // I/O
       if (blockIdI == blockIdJ) {
           byte[] buffer = getBlock(blockIdI);

           // Create a temporary buffer for the first record
           byte[] temp = new byte[FileGenerator.BYTES_PER_RECORD];

           // Swap records within the same buffer
           System.arraycopy(buffer, offsetI, temp, 0,
               FileGenerator.BYTES_PER_RECORD);
           System.arraycopy(buffer, offsetJ, buffer, offsetI,
               FileGenerator.BYTES_PER_RECORD);
           System.arraycopy(temp, 0, buffer, offsetJ,
               FileGenerator.BYTES_PER_RECORD);

           // Mark the buffer as dirty
           markDirty(blockIdI);
       }
       else {
           // Get blocks containing the records
           byte[] bufferI = getBlock(blockIdI);
           byte[] bufferJ = getBlock(blockIdJ);

           // Create temporary buffers for the records
           byte[] recordI = new byte[FileGenerator.BYTES_PER_RECORD];
           byte[] recordJ = new byte[FileGenerator.BYTES_PER_RECORD];

           // Copy records to temporary buffers
           System.arraycopy(bufferI, offsetI, recordI, 0,
               FileGenerator.BYTES_PER_RECORD);
           System.arraycopy(bufferJ, offsetJ, recordJ, 0,
               FileGenerator.BYTES_PER_RECORD);

           // Swap records
           System.arraycopy(recordJ, 0, bufferI, offsetI,
               FileGenerator.BYTES_PER_RECORD);
           System.arraycopy(recordI, 0, bufferJ, offsetJ,
               FileGenerator.BYTES_PER_RECORD);

           // Mark buffers as dirty
           markDirty(blockIdI);
           markDirty(blockIdJ);
       }
   }
}