-- shrsub -- submit a one node job to the share queue on cheyenne
--        -- and place the job on the share node with the fewest
--        -- jobs from the same user
{-# LANGUAGE DeriveDataTypeable, QuasiQuotes, FlexibleContexts #-}
module Lib
    ( libMain
    ) where

import System.Console.CmdArgs
import System.Process (readCreateProcess, readCreateProcessWithExitCode, shell)
import System.Exit (ExitCode(..))
import System.Posix.User (getLoginName)
import Text.Regex.PCRE.Heavy (split, scan, re, (=~))
import Data.List (nub, sort)
import Data.Char (isSpace)

--import Numeric.Statistics (average, avgdev)

data Shrsub = Shrsub { file_ :: FilePath
                     , show_ :: Bool
                     } deriving (Data, Typeable, Show, Eq)

shrsub = Shrsub
         { file_ = def &= args &= typ "FILE"
         , show_ = def &= name "s" &= help "Show the generated qsub line without submitting."
         } &=
         verbosity &=
         help "Wrapper for qsub to submit to least used node in share queue" &=
         summary "shrsub v0.0.1" &=
         details ["shrsub is a wrapper for qsub used for submitting jobs to the"
                 ,"share queue."
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
    let nosubmit = (show_ args)
    loud <- isLoud
    share_nodes <- getShareReservations 
                >>= (sequence . (map getShareNodes)) 
                >>= (return . nub . concat)
    down_nodes <- getDownNodes
    let avail_nodes = filter (`notElem` down_nodes) share_nodes
    nodeloads <- sequence $ getNodeLoads avail_nodes
    --let submit_host = (nodename (head (sort nodeloads))) 
    let submit_host = (nodename . head . sort) nodeloads
    let qsub_str = "qsub -l select=host=" ++ submit_host ++ " " ++ pbs_script

    -- extra output when verbose mode enabled
    whenLoud $ verboseOut share_nodes down_nodes avail_nodes 
                          nodeloads submit_host
   
    -- print the qsub line or submit the job
    if (nosubmit || loud) then putStrLn ("  Qsub line: " ++ qsub_str) else putStr []
    if (nosubmit) then putStr [] else do
      pbs_rval <- readCreateProcess (shell qsub_str) []
      putStrLn $ pbs_rval

-- return a list of the nodes in the given reservation
getShareNodes :: String -> IO [String]
getShareNodes res = do
    let cmd_str = "pbs_rstat -F " ++ res ++ ".chadmin1"
    shr_q_info <- readCreateProcess (shell cmd_str) []
    let node_str = head . filter (\x -> x =~ [re|^resv_nodes|]) $ lines shr_q_info
    let nodes = filter (\x -> not (x =~ [re|=|])) (split [re|[(:]|] node_str)
    return nodes

-- return list of PBS reservation strings being routed to by the share queue
getShareReservations :: IO [String]
getShareReservations = do
    shr_q_info <- readCreateProcess (shell "qstat -Qf share") []
    let res_dest_info = head . filter (\x -> x =~ [re|^\s+route_destinations|]) $ lines shr_q_info
    let shr_q_res = map fst $ scan [re|R[0-9]+|] res_dest_info
    return shr_q_res

-- return a list of the nodes in an offline state
getDownNodes :: IO [String]
getDownNodes = readCreateProcess (shell "pbsnodes -l") [] 
           >>= return . (map rmwhite) . lines
        where rmwhite s = filter (not . isSpace) $ take 10 s 

-- Get the number of this user's jobs on a particular node
getNodeLoad :: String -> IO NodeLoad
getNodeLoad node_str = do
    user_name <- getLoginName
    let cmd_str = "qstat -n -u " ++ user_name ++ " | grep " ++ node_str
    (rval, sout, serr) <- readCreateProcessWithExitCode (shell cmd_str) []
    let njobs = if rval == ExitSuccess then length (lines sout) else 0
    let node = NodeLoad { nodename = node_str, nodejobs = njobs }
    return node

-- Get Node Loads for all nodes in a list
getNodeLoads :: [String] -> [IO NodeLoad]
getNodeLoads [] = []
getNodeLoads (n:ns) = getNodeLoad n : getNodeLoads ns
    
-- extra output when verbose mode enabled
verboseOut :: [String] -> [String] -> [String] -> [NodeLoad] -> String -> IO ()
verboseOut sn dn an nl sh = putStrLn ("  Share Nodes: " ++ (show sn))
      >> putStrLn ("  Down Nodes: " ++ (show dn))
      >> putStrLn ("  Available Nodes: " ++ (show an))
      >> putStrLn ("  Node Loads: " ++ (show (sort nl)))
      >> putStrLn ("  Submit Job to node: " ++ sh)       

