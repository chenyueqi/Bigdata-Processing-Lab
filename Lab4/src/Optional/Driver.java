import java.io.IOException;
import java.util.*;

public class Driver {
	public static void main(String[] args) throws Exception {
		String[] forTo ={args[0],args[1]};
		StrongCheck.main(forTo);
		String[] forTo2 ={args[1],args[2]};
		DigraphToUngraph.main(forTo2);
		String[] forTo3 ={args[2],args[3]};
		InNeed.main(forTo3);
	}
}

