package kafka.context;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class PasswordHelper {
  private static SecretKeySpec secretKey;

  PasswordHelper(String myKey) {
    MessageDigest sha;
    try {
      byte[] key = myKey.getBytes(StandardCharsets.UTF_8);
      sha = MessageDigest.getInstance("SHA-256");
      key = sha.digest(key);
      key = Arrays.copyOf(key, 16);
      secretKey = new SecretKeySpec(key, "AES");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public static String generateKey() {
    SecureRandom random = new SecureRandom();
    byte[] salt = new byte[16];
    random.nextBytes(salt);
    return Base64.getEncoder().encodeToString(salt);
  }

  public String encrypt(String strToEncrypt) {
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return Base64.getEncoder()
          .encodeToString(cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      System.out.println("Error while encrypting: " + e);
    }
    return null;
  }

  public String decrypt(String strToDecrypt) {
    try {
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
    } catch (Exception e) {
      System.out.println("Error while decrypting: " + e);
    }
    return null;
  }

  public static void main(String[] args) {
    var salt = generateKey();
    System.out.println(salt);
    var h = new PasswordHelper(salt);
    var enc = h.encrypt("N2gFzDeGSaxowXBZVY97V16kuXl0o/hlP1KhLnO9muDqgB1TsL/DxwNVJacMJlhE");
    var dec = h.decrypt(enc);
    System.out.println(dec);
  }
}
