FROM perl
RUN cpanm --notest Log::Log4perl && cpanm --notest Module::Build
RUN cpanm --notest Graph
WORKDIR /usr/src/join-hero
ARG TEST_AUTHOR=1
RUN cpanm --notest Test::Code::TidyAll && cpanm --notest Perl::Tidy && cpanm --notest Test::Perl::Critic
COPY . /usr/src/join-hero
RUN tidyall -a
RUN cpanm --verbose .
ENTRYPOINT [ "join-hero" ]
LABEL name=join-hero maintainer="Caleb Hankins <caleb.hankins@acxiom.com>"
