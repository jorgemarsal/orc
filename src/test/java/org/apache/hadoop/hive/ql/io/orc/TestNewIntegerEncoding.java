package org.apache.hadoop.hive.ql.io.orc;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;

public class TestNewIntegerEncoding {

	public static class Row {
		Integer int1;
		Long long1;

		public Row(int val, long l) {
			this.int1 = val;
			this.long1 = l;
		}
	}

	public List<Long> fetchData(String path) throws IOException {
		List<Long> input = new ArrayList<Long>();
		FileInputStream stream = new FileInputStream(new File(path));
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
			/* Instead of using default, pass in a decoder. */
			String[] lines = Charset.defaultCharset().decode(bb).toString()
			    .split("\n");
			for (String line : lines) {
				long val = 0;
				try {
					val = Long.parseLong(line);
				} catch (NumberFormatException e) {
					// for now lets ignore (assign 0)
				}
				input.add(val);
			}
		} finally {
			stream.close();
		}
		return input;
	}

	Path workDir = new Path(System.getProperty("test.tmp.dir", "target"
	    + File.separator + "test" + File.separator + "tmp"));

	Configuration conf;
	FileSystem fs;
	Path testFilePath;
	String resDir = "src/test/resources";

	@Rule
	public TestName testCaseName = new TestName();

	@Before
	public void openFileSystem() throws Exception {
		conf = new Configuration();
		fs = FileSystem.getLocal(conf);
		testFilePath = new Path(workDir, "TestOrcFile."
		    + testCaseName.getMethodName() + ".orc");
		fs.delete(testFilePath, false);
	}

	@Test
	public void testBasicRow() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Row.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.addRow(new Row(111, 1111L));
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(new IntWritable(111), ((OrcStruct) row).getFieldValue(0));
			assertEquals(new LongWritable(1111), ((OrcStruct) row).getFieldValue(1));
		}
	}

	@Test
	public void testBasic() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		long[] inp = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
		    7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
		    2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
		    9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
		    1, 1, 1, 1 };
		List<Long> input = Lists.newArrayList(Longs.asList(inp));

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testIntegerMin() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		input.add((long) Integer.MIN_VALUE);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.ZLIB, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testIntegerMax() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		input.add((long) Integer.MAX_VALUE);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testLongMin() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		input.add(Long.MIN_VALUE);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testLongMax() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		input.add(Long.MAX_VALUE);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testRandomInt() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 100000; i++) {
			input.add((long) rand.nextInt());
		}

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testRandomLong() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 100000; i++) {
			input.add(rand.nextLong());
		}

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBaseAt0() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(0, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBaseAt1() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(1, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBaseAt255() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(255, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.ZLIB, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBaseAt256() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(256, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.ZLIB, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBase510() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(510, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.ZLIB, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testPatchedBase511() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 5120; i++) {
			input.add((long) rand.nextInt(100));
		}
		input.set(511, 20000L);

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.ZLIB, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 0;
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	@Test
	public void testSeek() throws Exception {
		ObjectInspector inspector;
		synchronized (TestOrcFile.class) {
			inspector = ObjectInspectorFactory.getReflectionObjectInspector(
			    Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		}

		List<Long> input = Lists.newArrayList();
		Random rand = new Random();
		for (int i = 0; i < 100000; i++) {
			input.add((long) rand.nextInt());
		}

		Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
		    CompressionKind.NONE, 10000, 10000);
		for (Long l : input) {
			writer.addRow(l);
		}
		writer.close();

		Reader reader = OrcFile.createReader(fs, testFilePath);
		RecordReader rows = reader.rows(null);
		int idx = 55555;
		rows.seekToRow(idx);
		while (rows.hasNext()) {
			Object row = rows.next(null);
			assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
		}
	}

	// NOTE: Following test cases needs github archive data in test resources
	// directory

	// @Test
	// public void testFromFile() throws Exception {
	// ObjectInspector inspector;
	// synchronized (TestOrcFile.class) {
	// inspector = ObjectInspectorFactory.getReflectionObjectInspector(
	// Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	// }
	//
	// String path = resDir + File.separator + "github" + File.separator
	// + "root.payload.id.txt";
	//
	// List<Long> input = fetchData(path);
	// Writer writer = OrcFile.createWriter(fs, testFilePath, inspector, 100000,
	// CompressionKind.ZLIB, 10000, 10000);
	// for (Long l : input) {
	// writer.addRow(l);
	// }
	// writer.close();
	//
	// Reader reader = OrcFile.createReader(fs, testFilePath);
	// RecordReader rows = reader.rows(null);
	// int idx = 0;
	// while (rows.hasNext()) {
	// Object row = rows.next(null);
	// assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
	// }
	// }
	//
	// private void runTest(String path) throws Exception {
	// ObjectInspector inspector;
	// synchronized (TestOrcFile.class) {
	// inspector = ObjectInspectorFactory.getReflectionObjectInspector(
	// Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
	// }
	//
	// String fullPath = resDir + File.separator + "github" + File.separator
	// + path;
	// Path tfp = new Path(System.getProperty("test.tmp.dir", "target"
	// + File.separator + "test" + File.separator + "tmp" + File.separator
	// + path));
	// List<Long> input = fetchData(fullPath);
	//
	// System.out.println("Running compression on " + path);
	// Writer writer = OrcFile.createWriter(fs, tfp, inspector, 100000,
	// CompressionKind.ZLIB, 10000, 10000);
	// for (Long l : input) {
	// writer.addRow(l);
	// }
	// writer.close();
	//
	// System.out.println("Running decompression on " + path);
	// Reader reader = OrcFile.createReader(fs, tfp);
	// RecordReader rows = reader.rows(null);
	// int idx = 0;
	// while (rows.hasNext()) {
	// Object row = rows.next(null);
	// assertEquals(input.get(idx++).longValue(), ((LongWritable) row).get());
	// }
	// }
	//
	// @Test
	// public void testGithubArchive() throws Exception {
	// File folder = new File(resDir + File.separator + "github");
	//
	// if (folder.exists()) {
	// File[] files = folder.listFiles();
	//
	// for (File file : files) {
	// runTest(file.getName());
	// }
	// } else {
	// System.out.println(folder.getCanonicalPath() + " folder doesn't exist.");
	// }
	// }
}
