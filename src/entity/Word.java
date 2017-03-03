package entity;

/**
 * Created by Tank
 *         on 2016/12/30.
 */
public class Word {

	private int id;
	private String word;
	private String ipa;
	private String explain;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getIpa() {
		return ipa;
	}

	public void setIpa(String ipa) {
		this.ipa = ipa;
	}

	public String getExplain() {
		return explain;
	}

	public void setExplain(String explain) {
		this.explain = explain;
	}

	@Override public String toString() {
		return id + "_" + word + "_" + ipa + "_" + explain;
	}
}
