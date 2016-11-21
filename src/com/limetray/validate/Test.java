package com.limetray.validate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Test {

	public static final String[] PREFIX_VALUES = new String[] { "contactus", "test", "noreply", "info", "mailus", "abc",
			"contact", "callcenter" };
	public static final Set<String> BLACKLISTED_PREFIX = new HashSet<String>(Arrays.asList(PREFIX_VALUES));

	public static final String[] SUFFIX_VALUES = new String[] { "test" };
	public static final Set<String> BLACKLISTED_SUFFIX = new HashSet<String>(Arrays.asList(SUFFIX_VALUES));

	public static boolean isEmailBackListed(String email) {
		int len = email.length();
		int index = email.lastIndexOf("@");
		String prefix = email.substring(0, index);
		String suffix = email.substring(index + 1, len);

		if (BLACKLISTED_SUFFIX.contains(suffix.toLowerCase())) {
			return true;
		} else if (BLACKLISTED_PREFIX.contains(prefix.toLowerCase())) {
			return true;
		}
		return false;
	}

	public static void main(String[] args) {
		String test = "Thifdfdffd@@@fdjfhj@test.com";
		System.out.println(isEmailBackListed("NOREPLY@gmail.com"));
	}

}
