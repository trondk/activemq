package com.custom.codec;

import org.apache.activemq.artemis.utils.SensitiveDataCodec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

public class AES256GCMCodec implements SensitiveDataCodec<String> {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int SALT_LENGTH = 16;
    private static final int IV_LENGTH = 12;
    private static final int GCM_TAG_BITS = 128;
    private static final int PBKDF2_ITERATIONS = 310_000;
    private static final int KEY_LENGTH_BITS = 256;

    private static final String PARAM_KEY = "key";
    private static final String SYSTEM_PROPERTY = "artemis.codec.key";
    private static final String ENV_VAR = "ARTEMIS_CODEC_KEY";

    private String masterPassword;

    @Override
    public void init(Map<String, String> params) throws Exception {
        // Try params first, then system property, then env var
        if (params != null && params.containsKey(PARAM_KEY)) {
            masterPassword = params.get(PARAM_KEY);
        } else if (System.getProperty(SYSTEM_PROPERTY) != null) {
            masterPassword = System.getProperty(SYSTEM_PROPERTY);
        } else if (System.getenv(ENV_VAR) != null) {
            masterPassword = System.getenv(ENV_VAR);
        }

        if (masterPassword == null || masterPassword.isEmpty()) {
            throw new IllegalArgumentException(
                "Master password not found. Provide via: " +
                "params[\"key\"], system property \"" + SYSTEM_PROPERTY + "\", " +
                "or env var \"" + ENV_VAR + "\"");
        }
    }

    @Override
    public String encode(Object secret) throws Exception {
        String plaintext = secret.toString();
        SecureRandom random = new SecureRandom();

        byte[] salt = new byte[SALT_LENGTH];
        random.nextBytes(salt);

        byte[] iv = new byte[IV_LENGTH];
        random.nextBytes(iv);

        SecretKey key = deriveKey(salt);

        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));

        byte[] ciphertext = cipher.doFinal(plaintext.getBytes("UTF-8"));

        // salt || iv || ciphertext (which includes the GCM auth tag)
        ByteBuffer buffer = ByteBuffer.allocate(SALT_LENGTH + IV_LENGTH + ciphertext.length);
        buffer.put(salt);
        buffer.put(iv);
        buffer.put(ciphertext);

        return Base64.getEncoder().encodeToString(buffer.array());
    }

    @Override
    public String decode(Object mask) throws Exception {
        // Replace spaces back to '+' in case URI parsing decoded '+' as space
        String input = mask.toString().replace(' ', '+');
        byte[] decoded = Base64.getDecoder().decode(input);
        ByteBuffer buffer = ByteBuffer.wrap(decoded);

        byte[] salt = new byte[SALT_LENGTH];
        buffer.get(salt);

        byte[] iv = new byte[IV_LENGTH];
        buffer.get(iv);

        byte[] ciphertext = new byte[buffer.remaining()];
        buffer.get(ciphertext);

        SecretKey key = deriveKey(salt);

        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));

        byte[] plaintext = cipher.doFinal(ciphertext);
        return new String(plaintext, "UTF-8");
    }

    @Override
    public boolean verify(char[] value, String encoded) {
        try {
            String decoded = decode(encoded);
            return decoded.equals(new String(value));
        } catch (Exception e) {
            return false;
        }
    }

    private SecretKey deriveKey(byte[] salt) throws Exception {
        PBEKeySpec spec = new PBEKeySpec(
            masterPassword.toCharArray(), salt, PBKDF2_ITERATIONS, KEY_LENGTH_BITS);
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            byte[] keyBytes = factory.generateSecret(spec).getEncoded();
            return new SecretKeySpec(keyBytes, "AES");
        } finally {
            spec.clearPassword();
        }
    }
}
