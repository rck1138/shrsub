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
import Text.Regex.PCRE.Heavy (split, scan, re, compileM,(=~))
import Data.List (nub, sort, intersperse)
import Data.Char (isSpace)
import Data.ByteString.Char8 (pack)
import Data.Either.Compat (fromRight)
import Control.Concurrent (threadDelay) 

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
                 ,"share queue. The job will be placed on the share node where the"
                 ,"user has the least number of currently running jobs."]

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
    loud <- isLoud
    let pbs_script = (file_ args)
    let nosubmit = (show_ args)
    submit_host <- numUnassigned >>= targetNode
    let qsub_str = "qsub -l select=host=" ++ submit_host ++ " " ++ pbs_script

    -- extra output when verbose mode enabled
    whenLoud $ verboseOut submit_host
   
    -- print the qsub line or submit the job
    if (nosubmit || loud) then putStrLn ("  Qsub line: " ++ qsub_str) else putStr []
    if (nosubmit) then putStr [] else do
      pbs_rval <- readCreateProcess (shell qsub_str) []
      putStrLn $ pbs_rval

-- Wait if there are currently unassigned jobs, otherwise return a 
-- target node to submit the job to
targetNode :: Int -> IO String
targetNode 0 = do
    share_nodes <- getShareNodes
    down_nodes <- getDownNodes
    let avail_nodes = filter (`notElem` down_nodes) share_nodes
    nodeloads <- sequence $ getNodeLoads avail_nodes
    return $ (nodename . head . sort) nodeloads
targetNode n = do
    putStrLn $ " " ++ show n ++ " unassigned share jobs. Retrying."
    threadDelay 2000000 >> numUnassigned >>= targetNode

-- return the number of unassigned jobs for this user in the share queue
numUnassigned :: IO Int
numUnassigned = do
    user_name <- getLoginName
    let cmd_str = "qstat -i -u " ++ user_name 
    shr_q_res <- getShareReservations
    let res_str = concat $ intersperse "|" shr_q_res
    let rex = fromRight [re||] $ compileM (pack (res_str)) []
    readCreateProcess (shell cmd_str) [] >>= return . length . filter (=~ rex) . lines

-- return list of PBS reservation strings being routed to by the share queue
getShareReservations :: IO [String]
getShareReservations = do
    shr_q_info <- readCreateProcess (shell "qstat -Qf share") []
    let res_dest_info = head . filter (\x -> x =~ [re|^\s+route_destinations|]) $ lines shr_q_info
    return . map fst $ scan [re|R[0-9]+|] res_dest_info

-- return a list of the nodes in the given reservation
getResNodes :: String -> IO [String]
getResNodes res = do
    let cmd_str = "pbs_rstat -F " ++ res ++ ".chadmin1"
    shr_q_info <- readCreateProcess (shell cmd_str) []
    let node_str = head . filter (=~ [re|^resv_nodes|]) $ lines shr_q_info
    return . filter (not . (=~ [re|=|])) $ split [re|[(:]|] node_str

-- get all nodes in all reservations routed to by share queue
getShareNodes :: IO [String]
getShareNodes = getShareReservations 
                >>= (sequence . (map getResNodes)) 
                >>= (return . nub . concat)

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
    let njobs = if rval == ExitSuccess then (length . lines) sout else 0
    return NodeLoad { nodename = node_str, nodejobs = njobs }

-- Get Node Loads for all nodes in a list
getNodeLoads :: [String] -> [IO NodeLoad]
getNodeLoads [] = []
getNodeLoads (n:ns) = getNodeLoad n : getNodeLoads ns
    
-- extra output when verbose mode enabled
verboseOut :: String -> IO ()
verboseOut submit_host = do
    share_nodes <- getShareNodes
    down_nodes <- getDownNodes
    let avail_nodes = filter (`notElem` down_nodes) share_nodes
    nodeloads <- sequence $ getNodeLoads avail_nodes
    putStrLn ("  Share Nodes: " ++ (show share_nodes))
      >> putStrLn ("  Down Nodes: " ++ (show down_nodes))
      >> putStrLn ("  Available Nodes: " ++ (show avail_nodes))
      >> putStrLn ("  Node Loads: " ++ (show (sort nodeloads)))
      >> putStrLn ("  Submit Job to node: " ++ submit_host)       

