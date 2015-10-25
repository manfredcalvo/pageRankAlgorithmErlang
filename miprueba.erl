-module(miprueba).

-compile(export_all).

% 1 + ((row-1) div K) * K + (col - 1) div K
% ?MODULE == Modulo actual

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


%Master Utils

bloquePertenece(Row, Col, N, K) -> N_K = N div K, 1 + ((Col - 1) div K) * N_K + (Row - 1) div K.

bloqueVectorPertenece(Block, N, K) -> N_K = N div K, 1  + ((Block - 1) div N_K).

writeTripletOnAFile(Block, Triplet) -> 
				file:write_file(lists:concat(["block", Block, "Matrix.csv"]), 
				io_lib:fwrite("~p.\n",[Triplet]), [append]).


writeVectorPairOnAFile(Block, Pair) -> 
					file:write_file(lists:concat(["block", Block, "Vector.csv"]), 
					io_lib:fwrite("~p.\n",[Pair]), [append]).

readResponse(0) -> ok;
readResponse(N) ->
	receive 
		{I, V} -> writeResultValueToKeyFile(I, V), readResponse(N);
		finish -> readResponse(N - 1)
	end.


writeResultValueToKeyFile(Key, Value) -> file:write_file(lists:concat(["Vector", Key, "Pos.csv"]), 
				io_lib:fwrite("~p.\n",[Value]), [append]).
 	
sendWorkToSlaves([]) -> ok;
sendWorkToSlaves([S|H]) -> S ! work, sendWorkToSlaves(H).


spawnMap([], _, _, Result)-> Result;
spawnMap([Block|T], N , K, Result) -> Args = [self(), lists:concat(["block", Block, "Matrix.csv"]), 
			lists:concat(["block", bloqueVectorPertenece(Block, N, K), "Vector.csv"])], spawnMap(T, N, K, [spawn(?MODULE, mapFunction, Args)|Result]).
				

%Slave Reduce Task

reduceFunction(Values, Master) -> Master ! reduce(Values, #{}).


reduce([], M) -> maps:to_list(M);
reduce([{K,V}|T], M) -> reduce(T, M#{K => reduceAux(V,0)}).

reduceAux([], S) -> S;
reduceAux([H|T], S) -> reduceAux(T, S + H).



%Slave Map Task

mapFunction(Master, MatrixFileName, VectorFileName) -> 
	receive
		work -> Master ! readMatrixFile(MatrixFileName, readVectorFile(VectorFileName), Master);
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
		Master ! {J, V * maps:get(J, Vector)},
		readMatrixFileAux(Device, Vector, Master) 
		
    end.


%Map Task Utils

stringToAtom(Sequence) -> {ok,Tokens,_EndLine} = erl_scan:string(Sequence),
			  {ok,AbsForm} = erl_parse:parse_exprs(Tokens),
			  {value,Value,_Bs} = erl_eval:exprs(AbsForm, erl_eval:new_bindings()),
			  Value.
