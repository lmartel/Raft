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

def interruptible
  begin
    yield
  rescue Interrupt
    puts
  end
end

task default: %w[compile docker:compile]

task :mkdirs do
    `mkdir -p build`
    `mkdir -p db`
    `mkdir -p log`
end

task :reset do
  `rm -f log/debug.*`
  `rm -f db/log.*`
end

task :compile => [:mkdirs, :reset] do
  sys compile(:osx)
end

task :log, [:server_id] => :mkdirs do |t, args|
  sys "./raft-osx #{args[:server_id]} log"
end

task :debug, [:server_id] => :mkdirs do |t, args|
  log = "log/debug.#{args[:server_id]}.log"
  `touch #{log}`
  interruptible { sys "clear; tail -100f #{log}" }
end

task :run, [:server_id] => :mkdirs do |t, args|
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
