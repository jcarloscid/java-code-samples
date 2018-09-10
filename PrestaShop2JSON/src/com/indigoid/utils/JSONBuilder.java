package com.indigoid.utils;

/**
 * This class is used to build a JSON representation of an object by appending
 * one by one the different elements of the JSON object. Method toString() is
 * provided to extract the final representation of the JSON object.
 *
 * Pretty sure that javax.json.JsonObjectBuilder is more powerful than this
 * class, but this has been build to practice Java.
 * 
 * @author Charlie
 *
 */
public class JSONBuilder {

	/**
	 * Unicode code point of the character " (double quotes)
	 */
	private static final int DOUBLE_QUOTE_CODEPOINT = 0x0022;
	/**
	 * The JSON object is built upon this string builder.
	 */
	private StringBuilder json;
	/**
	 * Outer padding of the JSON
	 */
	private String outPadding;
	/**
	 * Inner padding of the JSON elements.
	 */
	private String inPadding;
	/**
	 * JSON is written as one single line (as broken down into several lines)
	 */
	private boolean isOneLine;
	/**
	 * Flag used to prevent that when doing the continuation of previous object, a
	 * new comma became inserted.
	 */
	private boolean doNotAppendComma;

	/**
	 * Creates a new JSON builder.
	 * 
	 * @param level
	 *            Indentation level. If level is < 0 that means no indentation and
	 *            the JSON is generated as a single line.
	 */
	public JSONBuilder(int level) {

		// The string builder (will not contain the outer brackets).
		this.json = new StringBuilder();

		// Default values for line formating.
		this.outPadding = "";
		this.inPadding = " ";
		this.isOneLine = true;
		this.doNotAppendComma = false;

		// Compute values according to level
		if (level >= 0) {

			// Level > 0 means <level> tabs at the beginning of each line.
			if (level > 0) {
				char pad[] = new char[level];

				for (int i = 0; i < level; i++) {
					pad[i] = '\t';
				}
				this.outPadding = new String(pad);
			}

			// Inner padding is outer padding plus one more tab, therefore, each field is
			// listed in one single line.
			this.inPadding = this.outPadding + "\t";
			this.isOneLine = false;
		}
	}

	@Override
	public String toString() {
		String separator = (this.isOneLine) ? "" : "\n";
		return this.outPadding + "{" + separator + this.json.toString() + separator + this.outPadding + "}";
	}

	/**
	 * Resets the content of the builder and it became an empty builder again.
	 */
	public void reset() {
		json.delete(0, json.length());
		this.doNotAppendComma = false;
	}

	/**
	 * Replace the content from this instance using the content of the other
	 * instance. As a result, the new instance is a copy of the other instance,
	 * although they are two different instances.
	 * 
	 * @param other
	 *            JSONBuilder from which the content should be copied.
	 */
	public void copy(JSONBuilder other) {
		this.json.replace(0, this.json.length(), other.json.toString());
		this.outPadding = other.outPadding;
		this.inPadding = other.inPadding;
		this.isOneLine = other.isOneLine;
		this.doNotAppendComma = other.doNotAppendComma;
	}

	/**
	 * Appends a new integer element to the JSON
	 * 
	 * @param name
	 *            Field name
	 * @param value
	 *            Field value
	 */
	public void appendInt(String name, int value) {
		appendDecimal(name, new Integer(value).toString());
	}

	/**
	 * Appends a new String element to the JSON
	 * 
	 * @param name
	 *            Field name
	 * @param value
	 *            Field value
	 */
	public void appendString(String name, String value) {
		doContinuation();
		appendFieldName(name);
		json.append("\"");
		json.append(escapeCharacters(value));
		json.append("\"");
	}

	/**
	 * Escape it according to the RFC. JSON is pretty liberal: The only characters
	 * you must escape are \, ", and control codes (anything less than U+0020).
	 *
	 * This structure of escaping is specific to JSON. All of the escapes can be
	 * written as \\uXXXX where XXXX is the UTF-16 code unit for that character.
	 * 
	 * @param myString
	 *            String with possibly control characters.
	 * @return The new string with control characters escaped.
	 *
	 * @see http://www.ietf.org/rfc/rfc4627.txt
	 */
	private String escapeCharacters(String myString) {

		int len = myString.length();
		StringBuilder newString = new StringBuilder(len);
		for (int offset = 0; offset < len;) {
			int codePoint = myString.codePointAt(offset);
			offset += Character.charCount(codePoint);

			// Replace invisible control characters and unused code points
			switch (Character.getType(codePoint)) {
			case Character.CONTROL: // \p{Cc}
			case Character.FORMAT: // \p{Cf}
			case Character.PRIVATE_USE: // \p{Co}
			case Character.SURROGATE: // \p{Cs}
			case Character.UNASSIGNED: // \p{Cn}
				newString.append("\\u");
				newString.append(String.format("%04d", codePoint));
				break;
			default:
				if (codePoint == DOUBLE_QUOTE_CODEPOINT) 
					newString.append("\\"); // Escape the quotation mark " --> \" 
				newString.append(Character.toChars(codePoint));
				break;
			}
		}

		return newString.toString();
	}

	/**
	 * Appends a new Decimal element to the JSON
	 * 
	 * @param name
	 *            Field name
	 * @param value
	 *            Field value
	 */
	public void appendDecimal(String name, String value) {
		doContinuation();
		appendFieldName(name);
		json.append(value);
	}

	/**
	 * Appends a new Date/timestamp element to the JSON as an ISODate() object
	 * 
	 * @param name
	 *            Field name
	 * @param value
	 *            Field value
	 */
	public void appendDate(String name, String value) {
		doContinuation();
		appendFieldName(name);
		json.append("ISODate(\"");
		json.append(value);
		json.append("\")");
	}

	/**
	 * Opens an array representation in JSON format. Call closeArray() when done
	 * with inserting array elements.
	 * 
	 * @param name
	 *            Name of the field for current array.
	 */
	public void openArray(String name) {
		doContinuation();
		appendFieldName(name);
		json.append("[ ");
		doNotAppendComma = true;
	}

	/**
	 * Closes current array.
	 */
	public void closeArray() {
		json.append(" ]");
		doNotAppendComma = false;
	}

	/**
	 * Set the names of a property. This is typically done when the value of a property is a JSON document (instead of a primitive value).
	 * 
	 * @param name
	 *            Name of the field for current property.
	 */
	public void setProperty(String name) {
		doContinuation();
		appendFieldName(name);
		doNotAppendComma = true;
	}
	
	/**
	 * Appends to current JSON object a new element that indeed is another JSON
	 * object.
	 * 
	 * @param name
	 *            Name of the field being added
	 * @param jsonText
	 *            JSON representation of the object to be added.
	 */
	public void appendJSON(String name, String jsonText) {
		doContinuation();
		appendFieldName(name);
		json.append(jsonText);
	}

	/**
	 * Appends to current JSON object a new element that indeed is another JSON
	 * object. This version without field name is used to insert elements into an
	 * array.
	 * 
	 * @param jsonText
	 *            JSON representation of the object to be added.
	 */
	public void appendUnnamedJSON(String jsonText) {
		doContinuation();
		json.append(jsonText);
	}

	/**
	 * Manages element to element separators as well as line breaks.
	 */
	private void doContinuation() {
		if (json.length() > 0) {
			if (!doNotAppendComma) {
				json.append(",");
			} else {
				doNotAppendComma = false;
			}
			if (!this.isOneLine) {
				json.append("\n");
			}
		}
	}

	/**
	 * Appends the field name part of the JSON. Field names are always enclosed in
	 * double quotes.
	 * 
	 * @param name
	 *            Name of the field to be added
	 */
	private void appendFieldName(String name) {
		json.append(this.inPadding);
		json.append("\"");
		json.append(name);
		json.append("\": ");
	}
}