import java.io.IOException;
import java.util.*;

public class Driver
{
	public static void main(String[] args) throws Exception
	{
		String[] step1 ={"/data/task1/JN1_LOG", "./Pred/Step1"};
		Step1.main(step1);
		String[] step2 ={"./Pred/Step1", "./Pred/Step2"};
		Step2.main(step2);
	}
}
