import java.io.IOException;
import java.util.*;

public class Driver {
	public static void main(String[] args) throws Exception {
		String[] forTo ={args[0],args[1]};
		DigraphToUngraph.main(forTo);
		String[] forCN ={args[1],args[2]};
		InNeed.main(forCN);
	}
}

