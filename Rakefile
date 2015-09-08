require 'os'

mount = '/Users/leo/Dropbox/macdev/distributed_systems:/raft'

def ghc(os)
  if os == :linux then '/opt/ghc/7.10.2/bin/ghc' else 'ghc' end
end

def compile(os)
  "#{ghc os} -threaded -outputdir build -o raft-#{os} Main.hs"
end

def sys(arg_str)
  puts arg_str
  system(arg_str, out: $stdout, err: :out)
end

task default: %w[compile docker:compile]

task :compile do
    `mkdir -p build`
    sys compile(:osx)
end

task :run, [:server_id] do |t, args|
  sid = args[:server_id] || (puts "Missing arg: serverId" && exit(1))
  sys %Q[./raft-osx #{sid}]
end

namespace :docker do
  task :build do
    sys 'docker build -t raft .'
  end

  task :compile do
    sys %Q[docker run --rm -v #{mount} --name compile-raft raft /bin/sh -c "#{compile(:linux)}"]
  end

  task :run, [:server_id] do |t, args|
    sid = args[:server_id] || (puts "Missing arg: serverId" && exit(1))
    ports = ''
    # ports = "-p #{sid}3001:3001 -p #{sid}3002:3002 -p #{sid}3003:3003"
    begin
      `docker rm -f cohort#{sid}`
      sys %Q[docker run -t --rm #{ports} -v #{mount} --name cohort#{sid} raft /bin/sh -c "/raft/raft-linux #{sid}"]
    rescue SystemExit, Interrupt
      `docker kill -s INT cohort#{sid}`
    end
  end
end
