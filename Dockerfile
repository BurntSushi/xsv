#!/usr/bin/env -S docker build --compress -t xsv -f

FROM debian:10 as build

RUN apt update
RUN apt install -y \
	make cargo rustc

WORKDIR /data
COPY ./ ./

RUN make -j$(nproc) release

FROM debian:10
COPY --from=build /data/target/release/xsv /usr/local/bin

ENTRYPOINT [ "/usr/local/bin/xsv" ]
CMD        [ "--help" ]
