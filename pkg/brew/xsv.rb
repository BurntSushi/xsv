require  'formula'
class Xsv < Formula
  version '0.10.3'
  desc "Tool for proccessing CSV and delimited files"
  homepage "https://github.com/BurntSushi/xsv"

  if Hardware::CPU.is_64_bit?
    url "https://github.com/BurntSushi/xsv/releases/download/#{version}/xsv-#{version}-x86_64-apple-darwin.tar.gz"
    sha256 "ca74a0984d40f29810f5ad5d7e661151021a60528215e3294ce9c16ba2ea7025"
  else
    url "https://github.com/BurntSushi/xsv/releases/download/#{version}/xsv-#{version}-i686-apple-darwin.tar.gz"
    sha256 "206d14e0c1eac497ff14518941b231456f5db54937d004c57af5ac5c33f957ff"
  end

  def install
    bin.install "xsv"
    man1.install "xsv.1"
  end
end