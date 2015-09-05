task default: %w[compile]

task :compile do
    `mkdir -p build`
    `ghc -threaded -outputdir build -o raft Main.hs`
end 
