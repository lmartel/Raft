FROM phusion/passenger-ruby22
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:hvr/ghc
RUN apt-get update
RUN apt-get install -y git cabal-install-1.22 ghc-7.10.2
RUN export PATH=/opt/ghc/7.10.2/bin/:$PATH; cabal-1.22 update
RUN export PATH=/opt/ghc/7.10.2/bin/:$PATH; cabal-1.22 install network
RUN export PATH=/opt/ghc/7.10.2/bin/:$PATH; cabal-1.22 install HUnit
RUN export PATH=/opt/ghc/7.10.2/bin/:$PATH; cabal-1.22 install lens
RUN export PATH=/opt/ghc/7.10.2/bin/:$PATH; cabal-1.22 install aeson

WORKDIR /raft
COPY Gemfile /raft/Gemfile
RUN bundle install
