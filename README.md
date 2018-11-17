[![CircleCI](https://circleci.com/gh/kamatama41/embulk-filter-hash.svg?style=svg)](https://circleci.com/gh/kamatama41/embulk-filter-hash)

# Hash filter plugin for Embulk

Embulk filter plugin to convert an input to a hash value.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: Columns to hash (array, required)
  - **name**: Name of input column (string, required)
  - **algorithm**: Hash algorithm. [See also](#hash_algorithm) (string, default:`"SHA-256"`)
  - **secret_key**: Secret key for HMAC hashing. (string, required when specifying HMAC algorithm)
  - **new_name**: New column name if you want to rename the column (string, default: `null`)

## Example

```yaml
filters:
  - type: hash
    columns:
    - { name: username }
    - { name: email, algorithm: SHA-512, new_name: hashed_email }
    - { name: phone_number, algorithm: HmacSHA256, secret_key: passw0rd }
```

## Hash Algorithm
<a name ="hash_algorithm">

You can choose either of [MessageDigest](https://docs.oracle.com/javase/8/docs/api/java/security/MessageDigest.html) algorithm or [HMAC](https://docs.oracle.com/javase/8/docs/api/javax/crypto/Mac.html) algorithm.
If you want to know all algorithms that your platform supports, run the following snippet.

```java
for (String algorithm : java.security.Security.getAlgorithms("MessageDigest")) {
    System.out.println(algorithm);
}
for (String algorithm : java.security.Security.getAlgorithms("Mac")) {
    System.out.println(algorithm);
}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
