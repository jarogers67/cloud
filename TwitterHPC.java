import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import mpi.*;

// TwitterHPC
// Written by James Rogers (542046) <jarogers@student.unimelb.edu.au>
// Reads through a given Twitter csv file using MPI to distribute data across
// multiple processes. Counts the number of instances of a given search term
// as well as the top 10 Tweeters and topics within the Tweet texts.
public class TwitterHPC {

	// The data file to be searched on
	public static final String FILENAME = "twitter.csv";
	
	// A particular word to find matches for
	public static final String SEARCH_TERM = "kanye";

	// Constant definitions
	public static final int MASTER = 0;
	public static final int TWEETER = 1;
	public static final int TOPIC = 2;

    public static void main(String[] args) throws Exception {

    	// Begin timer
    	long startTime = System.currentTimeMillis();

    	// Set up MPI
    	MPI.Init(args);
    	int rank = MPI.COMM_WORLD.Rank();
    	int size = MPI.COMM_WORLD.Size();

    	// Search term to be counted
    	String searchTerm = SEARCH_TERM;
    	int searchTermCount = 0;

    	// Hash maps to find the most popular tweeters and topics
    	HashMap<String, Integer> tweeters = new HashMap<String, Integer>();
    	HashMap<String, Integer> topics = new HashMap<String, Integer>();

    	// Scan over the data file, skipping the first line
    	Scanner fileScanner = new Scanner(new FileInputStream(FILENAME));
		fileScanner.nextLine();

		// Count the number of lines while scanning through the file
		int lineNumber = 0;
    	while(fileScanner.hasNextLine()) {

    		String line = fileScanner.nextLine();
			Scanner lineScanner = new Scanner(line);
			lineNumber++;

    		try {

    			// Split the data between each process
    			if(lineNumber % size == rank) {

    				// Skip the first 6 sections, comma delimited
    				lineScanner.useDelimiter(",");
    				for(int i = 1; i <= 6; i++) {
    					lineScanner.next();
    				}

    				// Get the text component
    				lineScanner.useDelimiter("\"\",\"\"");
    				String text = lineScanner.next();
    				lineScanner.close();

    				// Remove JSON header, most punctuation and upper case
    				text = text.replaceFirst(",\"\"text\"\":\"\"", "");
    				text = text.replaceAll("[^A-Za-z0-9_#@ ]", " ");
    				text = text.toLowerCase();

    				// Scan through each word
    				Scanner textScanner = new Scanner(text);
    				while(textScanner.hasNext()) {
    					String word = textScanner.next();

    					// If a word starts with # or @, add it to the appropriate
    					// hash map or increment its count
    					if(word.charAt(0) == '#' && word.length() > 1) {
    						if(topics.containsKey(word)) {
    							topics.put(word, topics.get(word) + 1);
    						} else {
    							topics.put(word, 1);
    						}
    					} else if(word.charAt(0) == '@' && word.length() > 1) {
    						if(tweeters.containsKey(word)) {
    							tweeters.put(word, tweeters.get(word) + 1);
    						} else {
    							tweeters.put(word, 1);
    						}
    					}

    					if(word.equals(searchTerm)) {
    						searchTermCount++;
    					}

    				}

    				textScanner.close();

    			}

    		// Ignore any lines that are abnormally formatted
    		} catch(Exception e) {
    			System.out.println("Error Processing Line");
    		}

    	}

    	fileScanner.close();

    	// Master process must receive and aggregate results from other processes
    	if(rank == MASTER) {

    		// For each process collect the search term count via MPI
    		int[] searchTermCountBuffer = new int[size];
    		searchTermCountBuffer[0] = searchTermCount;
    		for(int i = 1; i < size; i++) {
    			MPI.COMM_WORLD.Recv(searchTermCountBuffer, i, 1, MPI.INT, i, 0);
    		}

    		// Sum for the total count
        	int totalCount = 0;
    		for(int i = 0; i < size; i++) {
    			totalCount += searchTermCountBuffer[i];
    		}

    		// Start building the aggregated hash maps
    		HashMap<String, Integer> tweetersFinal = new HashMap<String, Integer>(tweeters);
    		HashMap<String, Integer> topicsFinal = new HashMap<String, Integer>(topics);

    		// For each slave process receive a tweet map and a topic map
    		// Add these to the respective hash maps
    		for(int i = 1; i < size; i++) {
    			HashMap<String, Integer> tweetHashMap = receiveHashMapFromSlave(i, TWEETER);
    			tweetersFinal = addHashMaps(tweetersFinal, tweetHashMap);

    			HashMap<String, Integer> topicHashMap = receiveHashMapFromSlave(i, TOPIC);
    			topicsFinal = addHashMaps(topicsFinal, topicHashMap);
    		}

        	// Print the results at the master process
        	System.out.println("\"" + searchTerm + "\" appears " + totalCount + " times");
        	System.out.println();

        	System.out.println("Top 10 Tweeters");
        	printSummary(tweetersFinal, 10);
        	System.out.println();

        	System.out.println("Top 10 Topics");
        	printSummary(topicsFinal, 10);
        	System.out.println();

        	long endTime = System.currentTimeMillis();
        	System.out.println((endTime - startTime) / 1000 + " seconds taken");


        // For all other processes
    	} else {

    		// Send an array of the search term count to the master using MPI
    		int[] buffer = {searchTermCount};
    		MPI.COMM_WORLD.Send(buffer, 0, 1, MPI.INT, MASTER, 0);

    		// Send the hash maps of the tweeters and the topics to the master
    		sendHashMapToMaster(tweeters, TWEETER);
    		sendHashMapToMaster(topics, TOPIC);

    	}

    	MPI.Finalize();

    }

    // Receives a hash map from the given process of the given type using MPI
    public static HashMap<String, Integer> receiveHashMapFromSlave(int slaveRank, int type) {

    	// Find the byte count of the incoming array and build a buffer for it
		Status status = MPI.COMM_WORLD.Probe(slaveRank, type);
		int bufferLength = status.Get_count(MPI.BYTE);
		byte[] buffer = new byte[bufferLength];

		// Receive the byte array
		MPI.COMM_WORLD.Recv(buffer, 0, bufferLength, MPI.BYTE, slaveRank, type);

		// Convert from a byte input stream to an object input stream to the hash map, and return it
		try {
			ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(buffer);
			ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			HashMap<String, Integer> hashMap = (HashMap<String, Integer>)objectInputStream.readObject();

			return hashMap;
		} catch(Exception e) {
			return null;
		}

    }

    // Sends a hash map of the the given type to the master process using MPI
    public static void sendHashMapToMaster(HashMap<String, Integer> hashMap, int type) {

    	// Write the hash map object to a byte array buffer
    	byte[] buffer = null;
    	try {
    		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
    		objectOutputStream.writeObject(hashMap);
    		objectOutputStream.close();
    		buffer = byteArrayOutputStream.toByteArray();
    	} catch(Exception e) {}

    	// Send the buffer to the master process
    	MPI.COMM_WORLD.Send(buffer, 0, buffer.length, MPI.BYTE, MASTER, type);

    }

    // Adds the values of hash map B to hash map A
    public static HashMap<String, Integer> addHashMaps(HashMap<String, Integer> mapA, HashMap<String, Integer> mapB) {

    	// Convert the additional hash map to a list
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(mapB.entrySet());
		int listSize = list.size();

		// For each mapping in the list
		for(int i = 0; i < listSize; i++) {

			Map.Entry<String, Integer> entry = list.get(i);
			String key = entry.getKey();
			int value = entry.getValue();

			// Add the count to the hash map if already there
			if(mapA.containsKey(key)) {
				mapA.put(key, mapA.get(key) + value);
			} else {
				mapA.put(key, value);
			}

		}

		// Returns the combined hash map
		return mapA;

    }

    // Prints a ranked list of a given hash map to n places
    public static void printSummary(HashMap<String, Integer> hashMap, int n) {

    	// Define a comparator to order the hash map entries
    	Comparator<Map.Entry<String, Integer>> comparator = new Comparator<Map.Entry<String, Integer>>() {

    		public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
                return a.getValue() - b.getValue();
            }

    	};

    	// Convert to a list
    	List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(hashMap.entrySet());

    	// Sort and reverse the list to get highest first
    	Collections.sort(list, comparator);
    	Collections.reverse(list);

    	// Print the results
    	for(int i = 1; i <= n; i++) {
    		System.out.println(i + ": " + list.get(i - 1).getKey() + " (" + list.get(i - 1).getValue() + ")");
    	}

    }

}
