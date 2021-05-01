package wdc.productcorpus.v2.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;

import de.dwslab.dwslib.framework.Processor;

/**
 * Clears the content of all Files in a Folder specified by it's path.
 * 
 * @author Marc Becker
 *
 */
@Parameters(commandDescription = "")
public class ClearFile extends Processor<File>{
	
	@Parameter(
		names = { "-path" },
		required = true, description = "Path of the folder",
		converter = FileConverter.class
	)
	private File inputDirectory;

	@Override
	protected List<File> fillListToProcess() {
		List<File> files = new ArrayList<File>();
		for (File f : inputDirectory.listFiles()) {
			if (!f.isDirectory()) {
				files.add(f);
			}
		}
		return files;
	}

	@Override
	protected void process(File object) throws Exception {
		PrintWriter writer = new PrintWriter(object);
		writer.print("");
		writer.flush();
		writer.close();
		System.out.println("Cleared content of Files in: " + object.getPath());
	}

}
