package com.uebercomputing;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

public class FileRecordTest {

	@Test
	public void test() throws IOException {
		final Path inputDir = Paths.get("src/main/java/com/uebercomputing");
		final ByteBuffer byteBuffer = ByteBuffer.allocate(1 * 1024);
		FileRecord fileRecord = new FileRecord();
		final Map<CharSequence, CharSequence> fileMetadata = new HashMap<CharSequence, CharSequence>();
		fileRecord.setFileMetadata(fileMetadata);
		final FileRecord.Builder fileRecordBuilder = FileRecord
				.newBuilder(fileRecord);
		final DatumWriter<FileRecord> datumWriter = new SpecificDatumWriter<>();
		try (OutputStream out = Files.newOutputStream(Paths.get("files.avro"));
				DataFileWriter<FileRecord> writer = new DataFileWriter<>(
						datumWriter);) {
			writer.create(FileRecord.SCHEMA$, out);
			try (DirectoryStream<Path> directoryStream = Files
					.newDirectoryStream(inputDir)) {
				for (final Path path : directoryStream) {
					fileMetadata.clear();
					fileRecord = getFileRecord(fileRecordBuilder, fileMetadata,
							byteBuffer, path);
					writer.append(fileRecord);
				}
			}
		}
	}

	// see http://andreinc.net/2013/12/05/java-7-nio-2-tutorial-file-attributes/
	// for attributes
	private FileRecord getFileRecord(
			final FileRecord.Builder fileRecordBuilder,
			final Map<CharSequence, CharSequence> fileMetadata,
			final ByteBuffer byteBuffer, final Path path) throws IOException {
		final String fileName = path.getFileName().toString();
		final String directory = path.getParent().toString();
		final FileTime lastModifiedTime = (FileTime) Files.getAttribute(path,
				"basic:lastModifiedTime");
		final long size = (Long) Files.getAttribute(path, "basic:size");
		fileMetadata.put("size", "" + size);
		readUpToCapacityPathIntoBuffer(byteBuffer, path);
		fileRecordBuilder.setName(fileName).setDirectory(directory)
		.setLastModifiedTime(lastModifiedTime.toMillis())
		.setHeadOfContent(byteBuffer);
		final FileRecord fileRecord = fileRecordBuilder.build();
		return fileRecord;
	}

	// Only reads up to capacity bytes - just head of content
	private ByteBuffer readUpToCapacityPathIntoBuffer(
			final ByteBuffer byteBuffer, final Path path) throws IOException {
		try (ByteChannel byteChannel = Files.newByteChannel(path,
				StandardOpenOption.READ)) {
			byteChannel.read(byteBuffer);
		}
		byteBuffer.flip();
		return byteBuffer;
	}
}
