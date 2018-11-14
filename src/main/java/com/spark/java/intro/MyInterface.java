package com.spark.java.intro;

public interface MyInterface {

	default String hello() {
		return "Inside static method in interface";
	}

	void absmethod();
}
