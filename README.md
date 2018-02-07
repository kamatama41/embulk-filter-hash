[![CircleCI](https://circleci.com/gh/kamatama41/embulk-filter-hash.svg?style=svg)](https://circleci.com/gh/kamatama41/embulk-filter-hash)

# Hash filter plugin for Embulk

Embulk filter plugin to convert an input to a hash value.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: Columns to hash (array, required)
  - **name**: Name of input column (string, required)
  - **algorithm**: A hash algorithm. [See also](#hash_algorithm) (string, default:`"SHA-256"`)
  - **new_name**: New column name if you want to rename (string, default: `null`)

## Example

```yaml
filters:
  - type: hash
    columns:
    - { name: username }
    - { name: email, algorithm: SHA-512, new_name: hashed_email }
```

## Hash Algorithm
<a name ="hash_algorithm">

This plugin uses [MessageDigest](https://docs.oracle.com/javase/7/docs/api/java/security/MessageDigest.html) for hashing.
Every implementation of the Java platform supports the following MessageDigest algorithms:  
- MD5
- SHA-1
- SHA-256

If you want to know all algorithms that your platform supports, run the following snippet.
```java
for (String algorithm : java.security.Security.getAlgorithms("MessageDigest")) {
    System.out.println(algorithm);
}
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
