# `script` phase: you usually build, test and generate docs in this phase

set -ex

. $(dirname $0)/utils.sh

# NOTE Workaround for rust-lang/rust#31907 - disable doc tests when cross compiling
# This has been fixed in the nightly channel but it would take a while to reach the other channels
disable_cross_doctests() {
    if [ $(host) != "$TARGET" ] && [ "$TRAVIS_RUST_VERSION" = "stable" ]; then
        if [ "$TRAVIS_OS_NAME" = "osx" ]; then
            brew install gnu-sed --default-names
        fi

        find src -name '*.rs' -type f | xargs sed -i -e 's:\(//.\s*```\):\1 ignore,:g'
    fi
}

# TODO modify this function as you see fit
# PROTIP Always pass `--target $TARGET` to cargo commands, this makes cargo output build artifacts
# to target/$TARGET/{debug,release} which can reduce the number of needed conditionals in the
# `before_deploy`/packaging phase
run_test_suite() {
    case $TARGET in
        # configure emulation for transparent execution of foreign binaries
        aarch64-unknown-linux-gnu)
            export QEMU_LD_PREFIX=/usr/aarch64-linux-gnu
            ;;
        arm*-unknown-linux-gnueabihf)
            export QEMU_LD_PREFIX=/usr/arm-linux-gnueabihf
            ;;
        *)
            ;;
    esac

    if [ ! -z "$QEMU_LD_PREFIX" ]; then
        # Run tests on a single thread when using QEMU user emulation
        export RUST_TEST_THREADS=1
    fi

    cargo build --target $TARGET --verbose
    cargo test --target $TARGET

    # sanity check the file type
    file target/$TARGET/debug/xsv
}

main() {
    disable_cross_doctests
    run_test_suite
}

main
