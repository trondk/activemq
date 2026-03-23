# AES-256-GCM Codec for Apache Artemis

Drop-in replacement for Artemis's deprecated Blowfish codec. The JAR goes into the broker's `lib/` directory, you configure it in `broker.xml`, and the deprecation warning disappears.

## Build

```bash
mvn package
```

Produces `target/aes256gcm-codec-1.0.0.jar`.

## Install

Copy the JAR into your broker's `lib/` directory:

```bash
cp target/aes256gcm-codec-1.0.0.jar $ARTEMIS_INSTANCE/lib/
```

## Configure

Add to `broker.xml`:

```xml
<password-codec>com.custom.codec.AES256GCMCodec;key=YOUR_MASTER_PASSWORD</password-codec>
```

The master password can also be provided via:

- System property: `-Dartemis.codec.key=...`
- Environment variable: `ARTEMIS_CODEC_KEY`

Priority: params > system property > env var.

## Encrypt a password

```bash
java -cp "lib/*" org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec \
  --codec com.custom.codec.AES256GCMCodec \
  --key YOUR_MASTER_PASSWORD \
  --encode YOUR_SECRET
```

Then use the output in `broker.xml` with the `ENC()` wrapper:

```xml
<password>ENC(base64_output_here)</password>
```

## How it works

| Step | Detail |
|------|--------|
| Key derivation | PBKDF2-HMAC-SHA256, 310 000 iterations |
| Encryption | AES-256-GCM with 128-bit auth tag |
| Salt | 16 bytes, random per encode |
| IV | 12 bytes, random per encode |
| Wire format | `Base64(salt ‖ iv ‖ ciphertext ‖ tag)` |

Each call to `encode` produces a different ciphertext (random salt + IV), but `decode` always recovers the original plaintext.
