all:
	@echo Nothing to do...

ctags:
	ctags --recurse --options=ctags.rust --languages=Rust

docs:
	cargo doc
	in-dir ./target/doc fix-perms
	rscp ./target/doc/* gopher:~/www/burntsushi.net/rustdoc/

debug:
	cargo build --verbose
	rustc -L ./target/deps/ -g -Z lto --opt-level 3 src/main.rs

push:
	git push origin master
	git push github master

dev:
	cargo build
	cp ./target/xsv ~/bin/xsv

release:
	cargo build --release
	cp ./target/release/xsv ~/bin/xsv

