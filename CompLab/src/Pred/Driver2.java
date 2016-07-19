import java.io.IOException;
import java.util.*;

public class Driver1
{
	public static void main(String[] args) throws Exception
	{
		String[] step1 ={args[0],args[1]};
		Step1.main(step1);
		String[] step2 ={args[1],args[2]};
		Step2.main(step2);
	}
}
