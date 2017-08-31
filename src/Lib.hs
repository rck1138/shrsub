-- shrsub -- submit a one node job to the share queue on cheyenne
--        -- and place the job on the share node with the fewest
--        -- jobs from the same user
{-# LANGUAGE DeriveDataTypeable, QuasiQuotes, FlexibleContexts #-}
module Lib
    ( libMain
    ) where

import System.Console.CmdArgs
import System.IO
import System.Process
import System.Exit
import Text.Regex.PCRE.Heavy
import Data.List
import Data.Char

--import Numeric.Statistics (average, avgdev)

data Shrsub = Shrsub { file_ :: FilePath }
                     deriving (Data, Typeable, Show, Eq)

shrsub = Shrsub
--         { file_ = "std" &= args &= typ "FILE"
         { file_ = def &= args &= typ "FILE"
         } &=
         verbosity &=
         help "Wrapper for qsub to submit to least used node in share queue" &=
         summary "shrsub v0.0.1" &=
         details ["shrsub is a wrapper for qsub used for submitting jobs to the"
                 ,"share queue.","  Usage: shrsub batch_script"
                 ,"The job will be placed on the share node where the user"
                 ,"has the least number of currently running jobs."]

mode = cmdArgsMode shrsub

-- Data type for nodes orderable by # of user jobs
data NodeLoad = NodeLoad { nodename :: String
                         , nodejobs :: Int
                         } deriving (Eq, Show)
instance Ord NodeLoad where
    compare a b = compare (nodejobs a) (nodejobs b)

-- -- -- This function gets passed to main -- -- -- 
libMain :: IO ()
libMain = do
    args <- cmdArgs shrsub
    let pbs_script = (file_ args)
    share_nodes <- getShareNodes
    down_nodes <- getDownNodes
    let avail_nodes = filter (\x -> x `notElem` down_nodes) share_nodes
    nodeloads <- sequence $ getNodeLoads avail_nodes
    let submit_host = (nodename (head (sort nodeloads))) 
    let qsub_str = "qsub -l select=host=" ++ submit_host ++ " " ++ pbs_script
    -- extra output when verbose mode enabled
    whenLoud $
      putStrLn ("  Share Nodes: " ++ (show share_nodes))       >>
      putStrLn ("  Down Nodes: " ++ (show down_nodes))         >>
      putStrLn ("  Available Nodes: " ++ (show avail_nodes))   >>
      putStrLn ("  Node Loads: " ++ (show (sort nodeloads))) >>
      putStrLn ("  Submit Job to node: " ++ submit_host)       >>
      putStrLn ("  Qsub line: " ++ qsub_str)
       
    -- submit the job
    pbs_rval <- readCreateProcess (shell qsub_str) []
    putStrLn $ pbs_rval

-- return user name
getUserName :: IO String
getUserName = do 
    rval <- readCreateProcess (shell "whoami") ""
    return $ filter (/= '\n') rval

-- return a list of the nodes in the share queue
getShareNodes :: IO [String]
getShareNodes = do
    shr_q_info <- readCreateProcess (shell "pbs_rstat -F R1017978.chadmin1") []
    let node_str = head . filter (\x -> x =~ [re|^resv_nodes|]) $ lines shr_q_info
    let nodes = filter (\x -> not (x =~ [re|=|])) (split [re|[(:]|] node_str)
    return nodes

-- return a list of the nodes in an offline state
getDownNodes :: IO [String]
getDownNodes = do
    dn_str <- readCreateProcess (shell "pbsnodes -l") []
    let downnodes = map rmwhite $ lines dn_str
    return downnodes
        where rmwhite s = filter (not . isSpace) $ take 10 s 

-- Get the number of this user's jobs on a particular node
getNodeLoad :: String -> IO NodeLoad
getNodeLoad node_str = do
    user_name <- getUserName
    let cmd_str = "qstat -n -u " ++ user_name ++ " | grep " ++ node_str
    (rval, sout, serr) <- readCreateProcessWithExitCode (shell cmd_str) []
    let njobs = if rval == ExitSuccess then length (lines sout) else 0
    let node = NodeLoad { nodename = node_str, nodejobs = njobs }
    return node

-- Get Node Loads for all nodes in a list
getNodeLoads :: [String] -> [IO NodeLoad]
getNodeLoads [] = []
getNodeLoads (n:ns) = getNodeLoad n : getNodeLoads ns
    

