package superflow;

import org.apache.hadoop.util.ProgramDriver;

public class select {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("SFilterMR", SFilterMR.class, "Filter out 192.168 and white list.");

			pgd.addClass("SFlowMR", SFlowMR.class, "Merge flows with same pair without considering port & protocol");
			
			pgd.addClass("SFlowGroupMR", SFlowGroupMR.class, "Superflow group");
			
			pgd.addClass("LabelMR", LabelMR.class, "Give group number ID");
			
			pgd.driver(argv);

			exitCode = 0;
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
