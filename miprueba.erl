-module(miprueba).

-compile(export_all).

% 1 + ((row-1) div K) * K + (col - 1) div K
% ?MODULE == Modulo actual
%http://www.blogjava.net/ivanwan/archive/2011/03/18/346552.html
%Master Functions

%sshfs -p 22 mcalvo@192.168.0.3:/home/mcalvo/pageRankFolder ~/pageRankFolder -oauto_cache,reconnect

-define(FOLDER, "/home/mcalvo/pageRankFolder/").

separateMatrixOnBlocks(Name, N, K) ->
    {ok, Device} = file:open(Name,[read]),   
    readInputMatrixFile(Device, 1, N, K, sets:new()).

readInputMatrixFile(Device, S, N, K, Workers) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device), sets:to_list(Workers);
        Line -> Neighboors = string:tokens(Line,","),
		readInputMatrixFile(Device, S + 1, N, K, visitNeighboors(Neighboors, S, length(Neighboors), N, K, Workers)) 
		
    end.

visitNeighboors([], _, _, _, _, Workers) -> Workers;
visitNeighboors([H|T], S, L, N, K, Workers) ->
				{A,_} =  string:to_integer(H),
				Triplet = {A,S,1/L},
				Block = bloquePertenece(A, S, N, K),
				writeTripletOnAFile(Block, Triplet),
				visitNeighboors(T, S, L, N, K, sets:add_element(Block, Workers)).



separateVectorOnBlocks(FileName, K) -> {ok, Device} = file:open(FileName, [read]),
				    readInputVectorFile(Device, K).

readInputVectorFile(Device, K) -> 
			case io:get_line(Device, "") of
				eof -> file:close(Device);
				Line -> {I,V} = stringToAtom(Line), writeVectorPairOnAFile(((I - 1) div K) + 1, {I,V}), readInputVectorFile(Device, K)  
			end.


pageRank(MatrixFileName, VectorFileName, N, K, Iterations, Beta, Nodes) ->	
								removeAndCreateDir(?FOLDER ++ "MatrixBlocks"),
								MatrixBlocks = separateMatrixOnBlocks(MatrixFileName, N, K),
								MapSlaves = spawnMap(MatrixBlocks, Nodes, 1, N, K, []),
							        ReduceSlaves = spawnReduce(1, (N div K) + 1, 1, K, {Beta, N}, Nodes, 1, []),
								pageRankAux(VectorFileName, N, K, Iterations, 0, MapSlaves, ReduceSlaves),
								sendByeToProcess(MapSlaves),
								sendByeToProcess(ReduceSlaves).
								

pageRankAux(_, _, _, Iterations, Iterations,  _, _) -> ok;
pageRankAux(VectorFileName, N, K, Iterations, Actual, MapSlaves, ReduceSlaves) -> 
						 removeAndCreateDir(?FOLDER ++ "VectorBlocks"),
						 removeAndCreateDir(?FOLDER ++ "VectorPos"),
						 separateVectorOnBlocks(VectorFileName, K),
						 sendWorkToSlaves(MapSlaves),
						 readResponseMap(length(MapSlaves)),
						 sendWorkToReducers(ReduceSlaves),
						 readResponseReduce(N, Actual),
						 pageRankAux(getVectorInputName(Actual), N, K, Iterations, 1 + Actual, MapSlaves, ReduceSlaves).

removeAndCreateDir(Dir) -> os:cmd("rm -Rf " ++ Dir),
				 os:cmd("mkdir " ++ Dir).


						
 
 

%Master Utils

bloquePertenece(Row, Col, N, K) -> N_K = N div K, 1 + ((Col - 1) div K) * N_K + (Row - 1) div K.

bloqueVectorPertenece(Block, N, K) -> N_K = N div K, 1  + ((Block - 1) div N_K).

writeTripletOnAFile(Block, Triplet) -> 
				file:write_file(lists:concat([?FOLDER ++ "MatrixBlocks/block", Block, "Matrix.csv"]), 
				io_lib:fwrite("~p.\n",[Triplet]), [append]).


writeVectorPairOnAFile(Block, Pair) -> 
					file:write_file(lists:concat([?FOLDER ++ "VectorBlocks/block", Block, "Vector.csv"]), 
					io_lib:fwrite("~p.\n",[Pair]), [append]).

readResponseReduce(0, _) -> ok;
readResponseReduce(N, Iteration) ->
			receive 
				{I, V} -> writeResultValueToResultFile({I,V}, Iteration), readResponseReduce(N - 1, Iteration)
			end.


readResponseMap(0) -> ok;
readResponseMap(N) ->
	receive 
		{I, V} -> writeResultValueToKeyFile(I, V), readResponseMap(N);
		finish -> readResponseMap(N - 1)
	end.


writeResultValueToResultFile(Value, Iteration) -> file:write_file(getVectorInputName(Iteration), 
				io_lib:fwrite("~p.\n",[Value]), [append]).

getVectorInputName(Iteration) -> lists:concat([?FOLDER ++ "VectorInput", Iteration, ".csv"]).

writeResultValueToKeyFile(Key, Value) -> file:write_file(lists:concat([?FOLDER ++ "VectorPos/Vector", Key, "Pos.csv"]), 
				io_lib:fwrite("~p.\n",[Value]), [append]).
 	
sendWorkToSlaves([]) -> ok;
sendWorkToSlaves([S|H]) -> S ! work, sendWorkToSlaves(H).

sendByeToProcess([]) -> ok;
sendByeToProcess([H|T]) -> H ! bye, sendByeToProcess(T).


spawnMap([], _, _ , _, _, Result)-> Result;
spawnMap([Block|T], Nodes, Next, N , K, Result) -> 
			Args = [self(), lists:concat([?FOLDER ++ "MatrixBlocks/block", Block, "Matrix.csv"]), 
			lists:concat([?FOLDER ++ "VectorBlocks/block", bloqueVectorPertenece(Block, N, K), "Vector.csv"])], 
			spawnMap(T, Nodes, (Next rem tuple_size(Nodes)) + 1, N, K, [spawn(element(Next, Nodes), ?MODULE, mapFunction, Args)|Result]).



sendWorkToReducers([]) -> ok;
sendWorkToReducers([H|T]) -> H ! work, sendWorkToReducers(T).
				

spawnReduce(Number, Number, _ ,  _, _, _, _, Result) -> Result;
spawnReduce(Number, Limit, Start, K, Parameters, Nodes, Next, Result) -> spawnReduce(Number + 1, Limit, Start + K, K, Parameters, Nodes, 
									(Next rem tuple_size(Nodes)) + 1 ,	
					[spawn(element(Next, Nodes), ?MODULE, reduceFunction,[self(), Start, K, Parameters])|Result]).

%Slave Reduce Task

reduceFunction(Master, Start, BlockSize, Parameters) -> 
			receive
				work -> processVectorEntries(Start, Start + BlockSize, Parameters, Master), reduceFunction(Master, Start, BlockSize, Parameters);
				bye -> ok
			end. 

processVectorEntries(N, N, _,  _) -> ok;
processVectorEntries(N, Limit, Parameters, Master) -> Value = readVectorFileReduce(getVectorPosFileName(N), Parameters), Master ! {N, Value}, 
						processVectorEntries(N + 1, Limit, Parameters, Master). 


getVectorPosFileName(Pos) -> lists:concat([?FOLDER ++ "VectorPos/Vector", Pos, "Pos.csv"]).

readVectorFileReduce(FileName, Parameters) -> {ok, Device} = file:open(FileName, [read]),
				  readVectorFileReduceAux(Device, Parameters, 0).

readVectorFileReduceAux(Device, Parameters={Beta, N}, Acumulator) ->
				case io:get_line(Device, "") of
					eof -> file:close(Device), (Beta * Acumulator) + (1 - Beta) / N;
					Line -> Value = stringToAtom(Line), readVectorFileReduceAux(Device, Parameters, Acumulator + Value)
				end.
				 


%Slave Map Task

mapFunction(Master, MatrixFileName, VectorFileName) -> 
	receive
		work -> Response = readMatrixFile(MatrixFileName, readVectorFile(VectorFileName), Master), 
				   Master ! Response, mapFunction(Master, MatrixFileName, VectorFileName);
		bye -> ok
	end.




readVectorFile(FileName) -> {ok, Device} = file:open(FileName, [read]),
			    readVectorFileAux(Device, #{}).

readVectorFileAux(Device, M) ->
		case io:get_line(Device, "") of
			eof -> file:close(Device), M;
			Line -> {I, V}  = stringToAtom(Line), readVectorFileAux(Device, M#{I => V})
		end.
			    

readMatrixFile(FileName, Vector, Master) -> {ok, Device} = file:open(FileName,[read]),
			    readMatrixFileAux(Device, Vector, Master).	

readMatrixFileAux(Device, Vector, Master) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device), finish;
        Line -> {I,J,V} = stringToAtom(Line),
		Master ! {I, V * maps:get(J, Vector)},
		readMatrixFileAux(Device, Vector, Master) 
		
    end.


%Map Task Utils

stringToAtom(Sequence) -> {ok,Tokens,_EndLine} = erl_scan:string(Sequence),
			  {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
			  {value,Value,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
			  Value.
