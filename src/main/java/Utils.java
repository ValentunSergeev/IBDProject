public class Utils {
    /**
     *  Simple polynomial hash function for strings
     */
    public static int hashOf(String string) {
        int hash = 7;
        for (int i = 0; i < string.length(); i++) {
            hash = hash * 31 + string.charAt(i);
        }

        return hash;
    }
}
