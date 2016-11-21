package com.limetray.validate;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;

import com.google.i18n.phonenumbers.NumberParseException;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber.PhoneNumber;

public class ValidateMobile {

	private static String DB_DRIVER = null;
	private static String DB_CONNECTION = null;
	private static String DB_USER = null;
	private static String DB_PASSWORD = null;
	private static int batchSize, maxRecords;

	public static void main(String[] argv) throws SQLException, ClassNotFoundException, IOException {

		readConfigFile();

		PreparedStatement selectPS = null;
		ResultSet rs = null;
		Connection connection = null;
		int validCount = 0, inValidCount = 0;
		Date start = new Date();
		System.out.println("Start time " + start);
		try {
			connection = getConnection();
			connection.setAutoCommit(false);

			for (int i = 1; i < maxRecords; i = i + batchSize) {
				int end = i + batchSize - 1;
				System.out.println("Processing data between id " + i + " and " + end);
				String selectQuery = "select brand_user_id, primary_mobile, country_code from brand_user where brand_user_id between ? and ?";
				selectPS = connection.prepareStatement(selectQuery);
				selectPS.setLong(1, i);
				selectPS.setLong(2, end);
				rs = selectPS.executeQuery();

				String updateQuery = "update brand_user set primary_mobile = ?, country_code = ?, old_mobile = ?, old_country_code = ?, is_valid = ?, validity_reason = ? where brand_user_id = ?";
				PreparedStatement updatePS = connection.prepareStatement(updateQuery);
				while (rs.next()) {
					String brandUserID = rs.getString(1);
					String oldMobile = rs.getString(2);
					String oldCode = rs.getString(3);

					PhoneNumber number = validateMobileNumber(oldCode, oldMobile);
					if (number != null) {
						updatePS.setString(1, number.getNationalNumber() + "");
						updatePS.setString(2, "+" + number.getCountryCode());
						updatePS.setBoolean(5, true);
						updatePS.setString(6, null);
						validCount++;
					} else {
						updatePS.setString(1, oldMobile);
						updatePS.setString(2, oldCode);
						updatePS.setBoolean(5, false);
						updatePS.setString(6, "Invalid Mobile");
						inValidCount++;
					}

					updatePS.setString(3, oldMobile);
					updatePS.setString(4, oldCode);
					updatePS.setString(7, brandUserID);

					updatePS.addBatch();
				}

				updatePS.executeBatch();
				System.out.println("Batch successfully processed.");
				updatePS.close();
			}
			System.out.println("Total valid " + validCount);
			System.out.println("Total inValid " + inValidCount);
			connection.commit();

			Date end = new Date();
			System.out.println("End time " + end);
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("Exception occurred, roll backing.");
			connection.rollback();
		} finally {

			if (connection != null) {
				connection.close();
			}
			if (selectPS != null) {
				selectPS.close();
			}
			if (rs != null) {
				rs.close();
			}
		}

	}

	public static Connection getConnection() throws SQLException, ClassNotFoundException {
		Connection connection = null;
		Class.forName(DB_DRIVER);
		connection = DriverManager.getConnection(DB_CONNECTION + "?rewriteBatchedStatements=true", DB_USER, DB_PASSWORD);
		return connection;
	}

	public static PhoneNumber validateMobileNumber(String countryCode, String mobile) {
		PhoneNumber parsedNumber = validateNumber(countryCode + mobile, "");
		if (parsedNumber != null) {
			return parsedNumber;
		} else {
			parsedNumber = validateNumber(mobile, "");
			if (parsedNumber != null) {
				return parsedNumber;
			} else {
				parsedNumber = validateNumber(countryCode + mobile, "IN");
				if (parsedNumber != null) {
					return parsedNumber;
				} else {
					parsedNumber = validateNumber(mobile, "IN");
					if (parsedNumber != null) {
						return parsedNumber;
					} else {
						return null;
					}
				}
			}
		}
	}

	private static PhoneNumber validateNumber(String number, String countryLocale) {
		PhoneNumberUtil phoneUtil = PhoneNumberUtil.getInstance();
		try {
			PhoneNumber parsedNumber = phoneUtil.parse(number, countryLocale);
			boolean isValid = phoneUtil.isValidNumber(parsedNumber);
			if (isValid) {
				return parsedNumber;
			}

		} catch (NumberParseException e) {
		}
		return null;
	}

	public static void readConfigFile() throws IOException {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream("config.properties");
			prop.load(input);
			DB_DRIVER = prop.getProperty("DB_DRIVER");
			DB_CONNECTION = prop.getProperty("DB_CONNECTION");
			DB_USER = prop.getProperty("DB_USER");
			DB_PASSWORD = prop.getProperty("DB_PASSWORD");
			batchSize = Integer.parseInt(prop.getProperty("batchSize"));
			maxRecords = Integer.parseInt(prop.getProperty("maxRecords"));
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				input.close();
			}
		}
	}
}
