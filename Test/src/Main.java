import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;


public class Main {

	public static void main(String[] args){

		String test = "sadie	henry";
		String[] vals = test.split("\\t");
		System.out.println(vals.length);
	}
}
