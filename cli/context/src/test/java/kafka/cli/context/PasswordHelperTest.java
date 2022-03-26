package kafka.cli.context;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PasswordHelperTest {

  @Test
  void shouldMatch() {
    var salt = PasswordHelper.generateKey();
    System.out.println(salt);
    var h = new PasswordHelper(salt);
    var strToEncrypt = "test/123/test";
    var enc = h.encrypt(strToEncrypt);
    var dec = h.decrypt(enc);
    System.out.println(dec);
    assertEquals(strToEncrypt, dec);
  }
}
