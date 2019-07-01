FROM perl
RUN cpanm --notest Log::Log4perl && cpanm --notest Module::Build
RUN cpanm --notest Graph
COPY . /usr/src/join-hero
WORKDIR /usr/src/join-hero
RUN cpanm --verbose .
ENTRYPOINT [ "join-hero" ]
LABEL name=join-hero maintainer="Caleb Hankins <caleb.hankins@acxiom.com>"
