-module(miprueba).

-compile(export_all).

% 1 + ((row-1) div K) * K + (col - 1) div K
% ?MODULE == Modulo actual
%http://www.blogjava.net/ivanwan/archive/2011/03/18/346552.html
%Master Functions

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


pageRank(MatrixFileName, VectorFileName, N, K, Iterations) -> 	removeAndCreateDir("MatrixBlocks"),
								MatrixBlocks = separateMatrixOnBlocks(MatrixFileName, N, K),
								MapSlaves = spawnMap(MatrixBlocks, N, K, []),
							        ReduceSlaves = spawnReduce(N, []),
								pageRankAux(VectorFileName, N, K, Iterations, 0, MapSlaves, ReduceSlaves),
								sendByeToProcess(MapSlaves),
								sendByeToProcess(ReduceSlaves).
								

pageRankAux(_, _, _, Iterations, Iterations,  _, _) -> ok;
pageRankAux(VectorFileName, N, K, Iterations, Actual, MapSlaves, ReduceSlaves) -> removeAndCreateDir("VectorBlocks"),
						 removeAndCreateDir("VectorPos"),
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
				file:write_file(lists:concat(["MatrixBlocks/block", Block, "Matrix.csv"]), 
				io_lib:fwrite("~p.\n",[Triplet]), [append]).


writeVectorPairOnAFile(Block, Pair) -> 
					file:write_file(lists:concat(["VectorBlocks/block", Block, "Vector.csv"]), 
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

getVectorInputName(Iteration) -> lists:concat(["VectorInput", Iteration, ".csv"]).

writeResultValueToKeyFile(Key, Value) -> file:write_file(lists:concat(["VectorPos/Vector", Key, "Pos.csv"]), 
				io_lib:fwrite("~p.\n",[Value]), [append]).
 	
sendWorkToSlaves([]) -> ok;
sendWorkToSlaves([S|H]) -> S ! work, sendWorkToSlaves(H).

sendByeToProcess([]) -> ok;
sendByeToProcess([H|T]) -> H ! bye, sendByeToProcess(T).


spawnMap([], _, _, Result)-> Result;
spawnMap([Block|T], N , K, Result) -> Args = [self(), lists:concat(["MatrixBlocks/block", Block, "Matrix.csv"]), 
			lists:concat(["VectorBlocks/block", bloqueVectorPertenece(Block, N, K), "Vector.csv"])], spawnMap(T, N, K, [spawn(?MODULE, mapFunction, Args)|Result]).


sendWorkToReducers([]) -> ok;
sendWorkToReducers([H|T]) -> H ! work, sendWorkToReducers(T).
				

spawnReduce(0, Result) -> Result;
spawnReduce(N, Result) -> spawnReduce(N - 1, [spawn(?MODULE, reduceFunction, [self(), lists:concat(["VectorPos/Vector", N, "Pos.csv"]), N])|Result]).

%Slave Reduce Task

reduceFunction(Master, VectorFileName, N) -> 
			receive
				work -> Value = readVectorFileReduce(VectorFileName), Master ! {N, Value}, reduceFunction(Master, VectorFileName, N);
				bye -> ok
			end. 

readVectorFileReduce(FileName) -> {ok, Device} = file:open(FileName, [read]),
				  readVectorFileReduceAux(Device, 0).

readVectorFileReduceAux(Device, Acumulator) ->
				case io:get_line(Device, "") of
					eof -> file:close(Device), Acumulator;
					Line -> Value = stringToAtom(Line), readVectorFileReduceAux(Device, Acumulator + Value)
				end.
				 


%Slave Map Task

mapFunction(Master, MatrixFileName, VectorFileName) -> 
	receive
		work -> Master ! readMatrixFile(MatrixFileName, readVectorFile(VectorFileName), Master), mapFunction(Master, MatrixFileName, VectorFileName);
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


%* maps:get(J, Vector)

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
