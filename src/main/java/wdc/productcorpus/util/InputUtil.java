package wdc.productcorpus.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;



public class InputUtil {

	public static InputStream getInputStream(File f) throws IOException {
		InputStream is;
		if (!f.isFile()) {
			throw new IOException("Inputfile is not a file but a directory.");
		}
		if (f.getName().endsWith(".gz")) {
			is = new GZIPInputStream(new FileInputStream(f));
		} else if (f.getName().endsWith(".zip")) {
			is = new ZipInputStream(new FileInputStream(f));
		} else if (f.getName().endsWith(".bz2")) {
			is = new BZip2CompressorInputStream(new FileInputStream(f));
			// this is necessary to get the next entry
			((ZipInputStream) is).getNextEntry();
		} else if (f.getName().endsWith(".xz")) {
			is = new XZCompressorInputStream(new FileInputStream(f));
		} else {
			is = new FileInputStream(f);
		}
		return is;
	}
	
	public static BufferedReader getBufferedReader(File f) throws IOException {

		return getBufferedReader(f, "utf-8");
	}
	
	public static BufferedReader getBufferedReader(File f, String encoding)
			throws IOException {
		
		return new BufferedReader(new InputStreamReader(getInputStream(f),
				encoding));
		
		
	}
}
